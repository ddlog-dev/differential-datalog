use range_interner as ri;

#[derive(Eq, PartialEq, Clone)]
pub struct range_mapper_Mapper<K: ri::RangeInternerKey, V: ri::RangeInternerVal<V>> {
    interner: ri::RangeInterner<K, V>
}

impl<K: ri::RangeInternerKey + std::fmt::Debug, V: ri::RangeInternerVal<V> + std::fmt::Debug> std::fmt::Debug for range_mapper_Mapper<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::fmt::Debug::fmt(&self.interner, f)
    }
}

impl<K: ri::RangeInternerKey + std::fmt::Debug, V: ri::RangeInternerVal<V> + std::fmt::Debug> std::fmt::Display for range_mapper_Mapper<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::fmt::Debug::fmt(&self.interner, f)
    }
}

impl<K: ri::RangeInternerKey, V: ri::RangeInternerVal<V>> Default for range_mapper_Mapper<K, V> {
    fn default() -> Self {
        range_mapper_Mapper {
            interner: ri::RangeInterner::new("default", std::ops::Range{start: V::one(), end: V::one()})
        }
    }
}

impl<K: ri::RangeInternerKey, V: ri::RangeInternerVal<V>> serde::Serialize for range_mapper_Mapper<K, V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.interner.name().as_str())
    }
}

impl<'de, K: ri::RangeInternerKey, V: ri::RangeInternerVal<V>> serde::Deserialize<'de> for range_mapper_Mapper<K, V> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Err(<D::Error as serde::de::Error>::custom("range_mapper.Mapper.deserialize() is not implemented"))
    }
}

impl<K: ri::RangeInternerKey, V: ri::RangeInternerVal<V>> FromRecord for range_mapper_Mapper<K, V> {
    fn from_record(val: &record::Record) -> Result<Self, String> {
        Err("range_mapper.Mapper.from_record() is not implemented".to_string())
    }
}

impl<K: ri::RangeInternerKey, V: ri::RangeInternerVal<V>> IntoRecord for range_mapper_Mapper<K, V> {
    fn into_record(self) -> record::Record {
        record::Record::String(self.interner.name())
    }
}

impl<K: ri::RangeInternerKey, V: ri::RangeInternerVal<V>> record::Mutator<range_mapper_Mapper<K,V>> for record::Record {
    fn mutate(&self, vec: &mut range_mapper_Mapper<K, V>) -> Result<(), String> {
        Err("range_mapper.Mapper.mutate() is not implemented".to_string())
    }
}

impl<K: ri::RangeInternerKey, V: ri::RangeInternerVal<V>> PartialOrd for range_mapper_Mapper<K, V>
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let sptr = self.interner.inner().as_ref() as *const std::sync::Mutex<_> as usize;
        let optr = other.interner.inner().as_ref() as *const std::sync::Mutex<_> as usize;
        sptr.partial_cmp(&optr)
    }
}

impl<K: ri::RangeInternerKey, V: ri::RangeInternerVal<V>> Ord for range_mapper_Mapper<K, V>
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let sptr = self.interner.inner().as_ref() as *const std::sync::Mutex<_> as usize;
        let optr = other.interner.inner().as_ref() as *const std::sync::Mutex<_> as usize;
        sptr.cmp(&optr)
    }
}

impl<K: ri::RangeInternerKey, V: ri::RangeInternerVal<V>> std::hash::Hash for range_mapper_Mapper<K, V> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (self.interner.inner().as_ref() as *const std::sync::Mutex<_>).hash(state);
    }
}

#[derive(Eq, PartialOrd, PartialEq, Ord, Clone, Hash)]
pub struct range_mapper_Mapping<K: ri::RangeInternerKey, V: ri::RangeInternerVal<V>> {
    intern: ri::RangeIntern<K, V>
}

impl<K: ri::RangeInternerKey + std::fmt::Debug, V: ri::RangeInternerVal<V> + std::fmt::Debug> std::fmt::Debug for range_mapper_Mapping<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        std::fmt::Debug::fmt(&self.intern, f)
    }
}

impl<K: ri::RangeInternerKey + std::fmt::Display + Clone, V: ri::RangeInternerVal<V> + std::fmt::Display> std::fmt::Display for range_mapper_Mapping<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("({}->{})", range_mapper_mkey(self), range_mapper_mval(self)))
    }
}

impl<K: ri::RangeInternerKey + Default + Clone, V: ri::RangeInternerVal<V>> Default for range_mapper_Mapping<K, V> {
    fn default() -> Self {
        range_mapper_get_mapping(&range_mapper_Mapper::default(), &K::default())
    }
}

impl<K: ri::RangeInternerKey, V: ri::RangeInternerVal<V>> serde::Serialize for range_mapper_Mapping<K, V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(<S::Error as serde::ser::Error>::custom("range_mapper.Mapping.serialize() is not implemented"))
    }
}

impl<'de, K: ri::RangeInternerKey, V: ri::RangeInternerVal<V>> serde::Deserialize<'de> for range_mapper_Mapping<K, V> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Err(<D::Error as serde::de::Error>::custom("range_mapper.Mapping.deserialize() is not implemented"))
    }
}

impl<K: ri::RangeInternerKey, V: ri::RangeInternerVal<V>> FromRecord for range_mapper_Mapping<K, V> {
    fn from_record(val: &record::Record) -> Result<Self, String> {
        Err("range_mapper.Mapping.from_record() is not implemented".to_string())
    }
}

impl<K: ri::RangeInternerKey + IntoRecord + Clone, V: ri::RangeInternerVal<V> + IntoRecord> IntoRecord for range_mapper_Mapping<K, V> {
    fn into_record(self) -> record::Record {
        record::Record::Tuple(vec![range_mapper_mkey(&self).into_record(), range_mapper_mval(&self).into_record()])
    }
}

impl<K: ri::RangeInternerKey, V: ri::RangeInternerVal<V>> record::Mutator<range_mapper_Mapping<K,V>> for record::Record {
    fn mutate(&self, vec: &mut range_mapper_Mapping<K, V>) -> Result<(), String> {
        Err("range_mapper.Mapping.mutate() is not implemented".to_string())
    }
}

pub fn range_mapper_new_mapper<K, V>(name: &String, start: &V, end: &V) -> range_mapper_Mapper<K, V>
where
K: ri::RangeInternerKey,
V: ri::RangeInternerVal<V>
{
    range_mapper_Mapper{
        interner: ri::RangeInterner::new(name.as_str(), std::ops::Range{start: *start, end: *end})
    }
}

pub fn range_mapper_get_mapping<K, V>(mapper: &range_mapper_Mapper<K, V>, k: &K) -> range_mapper_Mapping<K, V>
where
K: ri::RangeInternerKey + Clone,
V: ri::RangeInternerVal<V>
{
    range_mapper_Mapping {
        intern: mapper.interner.intern((*k).clone())
    }
}

pub fn range_mapper_mkey<K, V>(m: &range_mapper_Mapping<K, V>) -> K
where
K: ri::RangeInternerKey + Clone,
V: ri::RangeInternerVal<V>
{
    m.intern.key().clone()
}

pub fn range_mapper_mval<K, V>(m: &range_mapper_Mapping<K, V>) -> std_Option<V>
where
K: ri::RangeInternerKey,
V: ri::RangeInternerVal<V>
{
    option2std(m.intern.val())
}

pub fn range_mapper_mmapper_name<K, V>(m: &range_mapper_Mapping<K, V>) -> String
where
K: ri::RangeInternerKey,
V: ri::RangeInternerVal<V>
{
    m.intern.interner().name()
}
