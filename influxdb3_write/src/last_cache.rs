use std::{any::Any, collections::VecDeque, sync::Arc};

use arrow::{
    array::{
        ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
        TimestampNanosecondBuilder, UInt64Builder,
    },
    datatypes::{DataType, SchemaRef, TimeUnit},
    error::ArrowError,
};
use async_trait::async_trait;
use datafusion::{
    datasource::{TableProvider, TableType},
    execution::context::SessionState,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};
use hashbrown::{HashMap, HashSet};
use indexmap::IndexMap;
use schema::TIME_COLUMN_NAME;

use crate::{
    catalog::LastCacheSize,
    write_buffer::{FieldData, Row},
};

/// A three level hashmap storing Database -> Table -> Cache Name
///
/// Each table can have multiple caches.
type CacheMap = HashMap<String, HashMap<String, HashMap<String, LastCache>>>;

pub(crate) struct LastCacheProvider {
    cache_map: CacheMap,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("invalid cache size")]
    InvalidCacheSize,
}

/// A ring buffer holding a set of [`Row`]s
pub(crate) struct LastCache {
    count: LastCacheSize,
    key_columns: HashSet<String>,
    schema: SchemaRef,
    cache: IndexMap<String, CacheColumn>,
}

impl LastCache {
    /// Create a new [`LastCache`]
    pub(crate) fn new(
        count: usize,
        key_columns: impl IntoIterator<Item: Into<String>>,
        schema: SchemaRef,
    ) -> Result<Self, Error> {
        let cache = schema
            .fields()
            .iter()
            .map(|f| (f.name().to_string(), CacheColumn::new(f.data_type(), count)))
            .collect();
        Ok(Self {
            count: count.try_into().map_err(|_| Error::InvalidCacheSize)?,
            key_columns: key_columns.into_iter().map(Into::into).collect(),
            schema,
            cache,
        })
    }

    pub(crate) fn push(&mut self, row: Row) {
        let time_col = self
            .cache
            .get_mut(TIME_COLUMN_NAME)
            .expect("there should always be a time column");
        time_col.push(FieldData::Timestamp(row.time));
        for field in row.fields {
            if let Some(c) = self.cache.get_mut(&field.name) {
                c.push(field.value);
            }
        }
    }

    fn to_record_batch(&self) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            self.schema(),
            self.cache.iter().map(|(_, c)| c.data.as_array()).collect(),
        )
    }
}

#[async_trait]
impl TableProvider for LastCache {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        // TODO: need to handle filters on the key columns as predicates here
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let partitions = vec![vec![self.to_record_batch()?]];
        let mut exec = MemoryExec::try_new(&partitions, self.schema(), projection.cloned())?;

        let show_sizes = ctx.config_options().explain.show_sizes;
        exec = exec.with_show_sizes(show_sizes);

        Ok(Arc::new(exec))
    }
}

struct CacheColumn {
    size: usize,
    data: CacheColumnData,
}

impl CacheColumn {
    fn new(data_type: &DataType, size: usize) -> Self {
        Self {
            size,
            data: CacheColumnData::new(data_type, size),
        }
    }

    fn push(&mut self, field_data: FieldData) {
        if self.data.len() == self.size {
            self.data.pop_back();
        }
        self.data.push_front(field_data);
    }
}

#[derive(Debug)]
enum CacheColumnData {
    I64(VecDeque<i64>),
    U64(VecDeque<u64>),
    F64(VecDeque<f64>),
    String(VecDeque<String>),
    Bool(VecDeque<bool>),
    Time(VecDeque<i64>),
}

impl CacheColumnData {
    fn new(data_type: &DataType, size: usize) -> Self {
        match data_type {
            DataType::Boolean => Self::Bool(VecDeque::with_capacity(size)),
            DataType::Int64 => Self::I64(VecDeque::with_capacity(size)),
            DataType::UInt64 => Self::U64(VecDeque::with_capacity(size)),
            DataType::Float64 => Self::F64(VecDeque::with_capacity(size)),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Self::Time(VecDeque::with_capacity(size))
            }
            DataType::Utf8 => Self::String(VecDeque::with_capacity(size)),
            DataType::Dictionary(k, v) if **k == DataType::Int32 && **v == DataType::Utf8 => {
                Self::String(VecDeque::with_capacity(size))
            }
            _ => panic!("unsupported data type for last cache: {data_type}"),
        }
    }

    fn len(&self) -> usize {
        match self {
            CacheColumnData::I64(v) => v.len(),
            CacheColumnData::U64(v) => v.len(),
            CacheColumnData::F64(v) => v.len(),
            CacheColumnData::String(v) => v.len(),
            CacheColumnData::Bool(v) => v.len(),
            CacheColumnData::Time(v) => v.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            CacheColumnData::I64(v) => v.is_empty(),
            CacheColumnData::U64(v) => v.is_empty(),
            CacheColumnData::F64(v) => v.is_empty(),
            CacheColumnData::String(v) => v.is_empty(),
            CacheColumnData::Bool(v) => v.is_empty(),
            CacheColumnData::Time(v) => v.is_empty(),
        }
    }

    fn pop_back(&mut self) {
        match self {
            CacheColumnData::I64(v) => {
                v.pop_back();
            }
            CacheColumnData::U64(v) => {
                v.pop_back();
            }
            CacheColumnData::F64(v) => {
                v.pop_back();
            }
            CacheColumnData::String(v) => {
                v.pop_back();
            }
            CacheColumnData::Bool(v) => {
                v.pop_back();
            }
            CacheColumnData::Time(v) => {
                v.pop_back();
            }
        }
    }

    fn push_front(&mut self, field_data: FieldData) {
        match (field_data, self) {
            (FieldData::Timestamp(d), CacheColumnData::Time(v)) => v.push_front(d),
            (FieldData::Key(d), CacheColumnData::String(v)) => v.push_front(d),
            (FieldData::Tag(d), CacheColumnData::String(v)) => v.push_front(d),
            (FieldData::String(d), CacheColumnData::String(v)) => v.push_front(d),
            (FieldData::Integer(d), CacheColumnData::I64(v)) => v.push_front(d),
            (FieldData::UInteger(d), CacheColumnData::U64(v)) => v.push_front(d),
            (FieldData::Float(d), CacheColumnData::F64(v)) => v.push_front(d),
            (FieldData::Boolean(d), CacheColumnData::Bool(v)) => v.push_front(d),
            _ => panic!("invalid field data for cache column"),
        }
    }

    fn as_array(&self) -> ArrayRef {
        match self {
            CacheColumnData::I64(v) => {
                let mut b = Int64Builder::new();
                v.iter().for_each(|val| b.append_value(*val));
                Arc::new(b.finish())
            }
            CacheColumnData::U64(v) => {
                let mut b = UInt64Builder::new();
                v.iter().for_each(|val| b.append_value(*val));
                Arc::new(b.finish())
            }
            CacheColumnData::F64(v) => {
                let mut b = Float64Builder::new();
                v.iter().for_each(|val| b.append_value(*val));
                Arc::new(b.finish())
            }
            CacheColumnData::String(v) => {
                let mut b = StringBuilder::new();
                v.iter().for_each(|val| b.append_value(val));
                Arc::new(b.finish())
            }
            CacheColumnData::Bool(v) => {
                let mut b = BooleanBuilder::new();
                v.iter().for_each(|val| b.append_value(*val));
                Arc::new(b.finish())
            }
            CacheColumnData::Time(v) => {
                let mut b = TimestampNanosecondBuilder::new();
                v.iter().for_each(|val| b.append_value(*val));
                Arc::new(b.finish())
            }
        }
    }
}
