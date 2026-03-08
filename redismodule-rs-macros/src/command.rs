use common::AclCategory;
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::quote;
use syn::{
    braced, bracketed, parenthesized,
    parse,
    parse::{Parse, ParseStream},
    parse_macro_input, ItemFn, LitInt, LitStr, Token,
};

/// Helper to parse a comma-separated list inside brackets: [Item, Item, ...]
fn parse_bracket_list<T: Parse>(input: ParseStream) -> parse::Result<Vec<T>> {
    let content;
    bracketed!(content in input);
    let mut items = Vec::new();
    while !content.is_empty() {
        items.push(content.parse()?);
        let _ = content.parse::<Token![,]>();
    }
    Ok(items)
}

/// Helper to parse an optional comma-separated list inside brackets, or return None
/// if the field wasn't provided.
fn parse_optional_bracket_list<T: Parse>(input: ParseStream) -> parse::Result<Vec<T>> {
    parse_bracket_list(input)
}

/// Helper to parse a possibly-negative integer literal (handles `- 1` as `-1`).
fn parse_i32(input: ParseStream) -> parse::Result<i32> {
    if input.peek(Token![-]) {
        input.parse::<Token![-]>()?;
        let lit: LitInt = input.parse()?;
        let val: i32 = lit.base10_parse()?;
        Ok(-val)
    } else {
        let lit: LitInt = input.parse()?;
        lit.base10_parse()
    }
}

/// Helper to parse a possibly-negative i64 literal.
fn parse_i64(input: ParseStream) -> parse::Result<i64> {
    if input.peek(Token![-]) {
        input.parse::<Token![-]>()?;
        let lit: LitInt = input.parse()?;
        let val: i64 = lit.base10_parse()?;
        Ok(-val)
    } else {
        let lit: LitInt = input.parse()?;
        lit.base10_parse()
    }
}

/// Helper to parse a u32 literal.
fn parse_u32(input: ParseStream) -> parse::Result<u32> {
    let lit: LitInt = input.parse()?;
    lit.base10_parse()
}

#[derive(Debug)]
pub enum RedisCommandFlags {
    /// The command may modify the data set (it may also read from it).
    Write,

    /// The command returns data from keys but never writes.
    ReadOnly,

    /// The command is an administrative command (may change replication or perform similar tasks).
    Admin,

    /// The command may use additional memory and should be denied during out of memory conditions.
    DenyOOM,

    /// Don't allow this command in Lua scripts.
    DenyScript,

    /// Allow this command while the server is loading data. Only commands not interacting with the data set
    /// should be allowed to run in this mode. If not sure don't use this flag.
    AllowLoading,

    /// The command publishes things on Pub/Sub channels.
    PubSub,

    /// The command may have different outputs even starting from the same input arguments and key values.
    /// Starting from Redis 7.0 this flag has been deprecated. Declaring a command as "random" can be done using
    /// command tips, see https://redis.io/topics/command-tips.
    Random,

    /// The command is allowed to run on slaves that don't serve stale data. Don't use if you don't know what
    /// this means.
    AllowStale,

    /// Don't propagate the command on monitor. Use this if the command has sensitive data among the arguments.
    NoMonitor,

    /// Don't log this command in the slowlog. Use this if the command has sensitive data among the arguments.
    NoSlowlog,

    /// The command time complexity is not greater than O(log(N)) where N is the size of the collection or
    /// anything else representing the normal scalability issue with the command.
    Fast,

    /// The command implements the interface to return the arguments that are keys. Used when start/stop/step
    /// is not enough because of the command syntax.
    GetkeysApi,

    /// The command should not register in Redis Cluster since is not designed to work with it because, for
    /// example, is unable to report the position of the keys, programmatically creates key names, or any
    /// other reason.
    NoCluster,

    /// This command can be run by an un-authenticated client. Normally this is used by a command that is used
    /// to authenticate a client.
    NoAuth,

    /// This command may generate replication traffic, even though it's not a write command.
    MayReplicate,

    /// All the keys this command may take are optional
    NoMandatoryKeys,

    /// The command has the potential to block the client.
    Blocking,

    /// Permit the command while the server is blocked either by a script or by a slow module command, see
    /// RM_Yield.
    AllowBusy,

    /// The command implements the interface to return the arguments that are channels.
    GetchannelsApi,
}

impl Parse for RedisCommandFlags {
    fn parse(input: ParseStream) -> parse::Result<Self> {
        let ident: Ident = input.parse()?;
        match ident.to_string().as_str() {
            "Write" => Ok(Self::Write),
            "ReadOnly" => Ok(Self::ReadOnly),
            "Admin" => Ok(Self::Admin),
            "DenyOOM" => Ok(Self::DenyOOM),
            "DenyScript" => Ok(Self::DenyScript),
            "AllowLoading" => Ok(Self::AllowLoading),
            "PubSub" => Ok(Self::PubSub),
            "Random" => Ok(Self::Random),
            "AllowStale" => Ok(Self::AllowStale),
            "NoMonitor" => Ok(Self::NoMonitor),
            "NoSlowlog" => Ok(Self::NoSlowlog),
            "Fast" => Ok(Self::Fast),
            "GetkeysApi" => Ok(Self::GetkeysApi),
            "NoCluster" => Ok(Self::NoCluster),
            "NoAuth" => Ok(Self::NoAuth),
            "MayReplicate" => Ok(Self::MayReplicate),
            "NoMandatoryKeys" => Ok(Self::NoMandatoryKeys),
            "Blocking" => Ok(Self::Blocking),
            "AllowBusy" => Ok(Self::AllowBusy),
            "GetchannelsApi" => Ok(Self::GetchannelsApi),
            other => Err(syn::Error::new(ident.span(), format!("unknown command flag `{other}`"))),
        }
    }
}

impl From<&RedisCommandFlags> for &'static str {
    fn from(value: &RedisCommandFlags) -> Self {
        match value {
            RedisCommandFlags::Write => "write",
            RedisCommandFlags::ReadOnly => "readonly",
            RedisCommandFlags::Admin => "admin",
            RedisCommandFlags::DenyOOM => "deny-oom",
            RedisCommandFlags::DenyScript => "deny-script",
            RedisCommandFlags::AllowLoading => "allow-loading",
            RedisCommandFlags::PubSub => "pubsub",
            RedisCommandFlags::Random => "random",
            RedisCommandFlags::AllowStale => "allow-stale",
            RedisCommandFlags::NoMonitor => "no-monitor",
            RedisCommandFlags::NoSlowlog => "no-slowlog",
            RedisCommandFlags::Fast => "fast",
            RedisCommandFlags::GetkeysApi => "getkeys-api",
            RedisCommandFlags::NoCluster => "no-cluster",
            RedisCommandFlags::NoAuth => "no-auth",
            RedisCommandFlags::MayReplicate => "may-replicate",
            RedisCommandFlags::NoMandatoryKeys => "no-mandatory-keys",
            RedisCommandFlags::Blocking => "blocking",
            RedisCommandFlags::AllowBusy => "allow-busy",
            RedisCommandFlags::GetchannelsApi => "getchannels-api",
        }
    }
}

#[derive(Debug)]
pub enum RedisEnterpriseCommandFlags {
    /// A special enterprise only flag, make sure the commands marked with this flag will not be expose to
    /// user via `command` command or on slow log.
    ProxyFiltered,
}

impl Parse for RedisEnterpriseCommandFlags {
    fn parse(input: ParseStream) -> parse::Result<Self> {
        let ident: Ident = input.parse()?;
        match ident.to_string().as_str() {
            "ProxyFiltered" => Ok(Self::ProxyFiltered),
            other => Err(syn::Error::new(ident.span(), format!("unknown enterprise flag `{other}`"))),
        }
    }
}

impl From<&RedisEnterpriseCommandFlags> for &'static str {
    fn from(value: &RedisEnterpriseCommandFlags) -> Self {
        match value {
            RedisEnterpriseCommandFlags::ProxyFiltered => "_proxy-filtered",
        }
    }
}

#[derive(Debug)]
pub enum RedisCommandKeySpecFlags {
    /// Read-Only. Reads the value of the key, but doesn't necessarily return it.
    ReadOnly,

    /// Read-Write. Modifies the data stored in the value of the key or its metadata.
    ReadWrite,

    /// Overwrite. Overwrites the data stored in the value of the key.
    Overwrite,

    /// Deletes the key.
    Remove,

    /// Returns, copies or uses the user data from the value of the key.
    Access,

    /// Updates data to the value, new value may depend on the old value.
    Update,

    /// Adds data to the value with no chance of modification or deletion of existing data.
    Insert,

    /// Explicitly deletes some content from the value of the key.
    Delete,

    /// The key is not actually a key, but should be routed in cluster mode as if it was a key.
    NotKey,

    /// The keyspec might not point out all the keys it should cover.
    Incomplete,

    /// Some keys might have different flags depending on arguments.
    VariableFlags,
}

impl Parse for RedisCommandKeySpecFlags {
    fn parse(input: ParseStream) -> parse::Result<Self> {
        let ident: Ident = input.parse()?;
        match ident.to_string().as_str() {
            "ReadOnly" => Ok(Self::ReadOnly),
            "ReadWrite" => Ok(Self::ReadWrite),
            "Overwrite" => Ok(Self::Overwrite),
            "Remove" => Ok(Self::Remove),
            "Access" => Ok(Self::Access),
            "Update" => Ok(Self::Update),
            "Insert" => Ok(Self::Insert),
            "Delete" => Ok(Self::Delete),
            "NotKey" => Ok(Self::NotKey),
            "Incomplete" => Ok(Self::Incomplete),
            "VariableFlags" => Ok(Self::VariableFlags),
            other => Err(syn::Error::new(ident.span(), format!("unknown key spec flag `{other}`"))),
        }
    }
}

impl From<&RedisCommandKeySpecFlags> for &'static str {
    fn from(value: &RedisCommandKeySpecFlags) -> Self {
        match value {
            RedisCommandKeySpecFlags::ReadOnly => "READ_ONLY",
            RedisCommandKeySpecFlags::ReadWrite => "READ_WRITE",
            RedisCommandKeySpecFlags::Overwrite => "OVERWRITE",
            RedisCommandKeySpecFlags::Remove => "REMOVE",
            RedisCommandKeySpecFlags::Access => "ACCESS",
            RedisCommandKeySpecFlags::Update => "UPDATE",
            RedisCommandKeySpecFlags::Insert => "INSERT",
            RedisCommandKeySpecFlags::Delete => "DELETE",
            RedisCommandKeySpecFlags::NotKey => "NOT_KEY",
            RedisCommandKeySpecFlags::Incomplete => "INCOMPLETE",
            RedisCommandKeySpecFlags::VariableFlags => "VARIABLE_FLAGS",
        }
    }
}

#[derive(Debug)]
pub struct FindKeysRange {
    last_key: i32,
    steps: i32,
    limit: i32,
}

impl Parse for FindKeysRange {
    fn parse(input: ParseStream) -> parse::Result<Self> {
        let content;
        braced!(content in input);
        let mut last_key = None;
        let mut steps = None;
        let mut limit = None;
        while !content.is_empty() {
            let key: Ident = content.parse()?;
            content.parse::<Token![:]>()?;
            match key.to_string().as_str() {
                "last_key" => last_key = Some(parse_i32(&content)?),
                "steps" => steps = Some(parse_i32(&content)?),
                "limit" => limit = Some(parse_i32(&content)?),
                other => return Err(syn::Error::new(key.span(), format!("unknown field `{other}`"))),
            }
            let _ = content.parse::<Token![,]>();
        }
        Ok(Self {
            last_key: last_key.ok_or_else(|| content.error("missing field `last_key`"))?,
            steps: steps.ok_or_else(|| content.error("missing field `steps`"))?,
            limit: limit.ok_or_else(|| content.error("missing field `limit`"))?,
        })
    }
}

#[derive(Debug)]
pub struct FindKeysNum {
    key_num_idx: i32,
    first_key: i32,
    key_step: i32,
}

impl Parse for FindKeysNum {
    fn parse(input: ParseStream) -> parse::Result<Self> {
        let content;
        braced!(content in input);
        let mut key_num_idx = None;
        let mut first_key = None;
        let mut key_step = None;
        while !content.is_empty() {
            let key: Ident = content.parse()?;
            content.parse::<Token![:]>()?;
            match key.to_string().as_str() {
                "key_num_idx" => key_num_idx = Some(parse_i32(&content)?),
                "first_key" => first_key = Some(parse_i32(&content)?),
                "key_step" => key_step = Some(parse_i32(&content)?),
                other => return Err(syn::Error::new(key.span(), format!("unknown field `{other}`"))),
            }
            let _ = content.parse::<Token![,]>();
        }
        Ok(Self {
            key_num_idx: key_num_idx.ok_or_else(|| content.error("missing field `key_num_idx`"))?,
            first_key: first_key.ok_or_else(|| content.error("missing field `first_key`"))?,
            key_step: key_step.ok_or_else(|| content.error("missing field `key_step`"))?,
        })
    }
}

#[derive(Debug)]
pub enum FindKeys {
    Range(FindKeysRange),
    Keynum(FindKeysNum),
}

impl Parse for FindKeys {
    fn parse(input: ParseStream) -> parse::Result<Self> {
        let ident: Ident = input.parse()?;
        let content;
        parenthesized!(content in input);
        match ident.to_string().as_str() {
            "Range" => Ok(Self::Range(content.parse()?)),
            "Keynum" => Ok(Self::Keynum(content.parse()?)),
            other => Err(syn::Error::new(ident.span(), format!("unknown find_keys variant `{other}`"))),
        }
    }
}

#[derive(Debug)]
pub struct BeginSearchIndex {
    index: i32,
}

impl Parse for BeginSearchIndex {
    fn parse(input: ParseStream) -> parse::Result<Self> {
        let content;
        braced!(content in input);
        let mut index = None;
        while !content.is_empty() {
            let key: Ident = content.parse()?;
            content.parse::<Token![:]>()?;
            match key.to_string().as_str() {
                "index" => index = Some(parse_i32(&content)?),
                other => return Err(syn::Error::new(key.span(), format!("unknown field `{other}`"))),
            }
            let _ = content.parse::<Token![,]>();
        }
        Ok(Self {
            index: index.ok_or_else(|| content.error("missing field `index`"))?,
        })
    }
}

#[derive(Debug)]
pub struct BeginSearchKeyword {
    keyword: String,
    startfrom: i32,
}

impl Parse for BeginSearchKeyword {
    fn parse(input: ParseStream) -> parse::Result<Self> {
        let content;
        braced!(content in input);
        let mut keyword = None;
        let mut startfrom = None;
        while !content.is_empty() {
            let key: Ident = content.parse()?;
            content.parse::<Token![:]>()?;
            match key.to_string().as_str() {
                "keyword" => {
                    let lit: LitStr = content.parse()?;
                    keyword = Some(lit.value());
                }
                "startfrom" => startfrom = Some(parse_i32(&content)?),
                other => return Err(syn::Error::new(key.span(), format!("unknown field `{other}`"))),
            }
            let _ = content.parse::<Token![,]>();
        }
        Ok(Self {
            keyword: keyword.ok_or_else(|| content.error("missing field `keyword`"))?,
            startfrom: startfrom.ok_or_else(|| content.error("missing field `startfrom`"))?,
        })
    }
}

#[derive(Debug)]
pub enum BeginSearch {
    Index(BeginSearchIndex),
    Keyword(BeginSearchKeyword), // (keyword, startfrom)
}

impl Parse for BeginSearch {
    fn parse(input: ParseStream) -> parse::Result<Self> {
        let ident: Ident = input.parse()?;
        let content;
        parenthesized!(content in input);
        match ident.to_string().as_str() {
            "Index" => Ok(Self::Index(content.parse()?)),
            "Keyword" => Ok(Self::Keyword(content.parse()?)),
            other => Err(syn::Error::new(ident.span(), format!("unknown begin_search variant `{other}`"))),
        }
    }
}

#[derive(Debug)]
pub struct KeySpecArg {
    notes: Option<String>,
    flags: Vec<RedisCommandKeySpecFlags>,
    begin_search: BeginSearch,
    find_keys: FindKeys,
}

impl Parse for KeySpecArg {
    fn parse(input: ParseStream) -> parse::Result<Self> {
        let content;
        braced!(content in input);
        let mut notes = None;
        let mut flags = None;
        let mut begin_search = None;
        let mut find_keys = None;
        while !content.is_empty() {
            let key: Ident = content.parse()?;
            content.parse::<Token![:]>()?;
            match key.to_string().as_str() {
                "notes" => {
                    let lit: LitStr = content.parse()?;
                    notes = Some(lit.value());
                }
                "flags" => flags = Some(parse_bracket_list(&content)?),
                "begin_search" => begin_search = Some(content.parse()?),
                "find_keys" => find_keys = Some(content.parse()?),
                other => return Err(syn::Error::new(key.span(), format!("unknown field `{other}`"))),
            }
            let _ = content.parse::<Token![,]>();
        }
        Ok(Self {
            notes,
            flags: flags.ok_or_else(|| content.error("missing field `flags`"))?,
            begin_search: begin_search.ok_or_else(|| content.error("missing field `begin_search`"))?,
            find_keys: find_keys.ok_or_else(|| content.error("missing field `find_keys`"))?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandArgType {
    String,
    Integer,
    Double,
    Key,
    Pattern,
    UnixTime,
    PureToken,
    OneOf,
    Block,
}

impl Parse for CommandArgType {
    fn parse(input: ParseStream) -> parse::Result<Self> {
        let ident: Ident = input.parse()?;
        match ident.to_string().as_str() {
            "String" => Ok(Self::String),
            "Integer" => Ok(Self::Integer),
            "Double" => Ok(Self::Double),
            "Key" => Ok(Self::Key),
            "Pattern" => Ok(Self::Pattern),
            "UnixTime" => Ok(Self::UnixTime),
            "PureToken" => Ok(Self::PureToken),
            "OneOf" => Ok(Self::OneOf),
            "Block" => Ok(Self::Block),
            other => Err(syn::Error::new(ident.span(), format!("unknown arg type `{other}`"))),
        }
    }
}

impl From<CommandArgType> for u32 {
    fn from(arg_type: CommandArgType) -> Self {
        match arg_type {
            CommandArgType::String => 0,
            CommandArgType::Integer => 1,
            CommandArgType::Double => 2,
            CommandArgType::Key => 3,
            CommandArgType::Pattern => 4,
            CommandArgType::UnixTime => 5,
            CommandArgType::PureToken => 6,
            CommandArgType::OneOf => 7,
            CommandArgType::Block => 8,
        }
    }
}

#[derive(Debug, Clone)]
pub enum CommandArgFlags {
    None,
    Optional,
    Multiple,
    MultipleToken,
}

impl Parse for CommandArgFlags {
    fn parse(input: ParseStream) -> parse::Result<Self> {
        let ident: Ident = input.parse()?;
        match ident.to_string().as_str() {
            "None" => Ok(Self::None),
            "Optional" => Ok(Self::Optional),
            "Multiple" => Ok(Self::Multiple),
            "MultipleToken" => Ok(Self::MultipleToken),
            other => Err(syn::Error::new(ident.span(), format!("unknown arg flag `{other}`"))),
        }
    }
}

impl From<&CommandArgFlags> for &'static str {
    fn from(value: &CommandArgFlags) -> Self {
        match value {
            CommandArgFlags::None => "NONE",
            CommandArgFlags::Optional => "OPTIONAL",
            CommandArgFlags::Multiple => "MULTIPLE",
            CommandArgFlags::MultipleToken => "MULTIPLE_TOKEN",
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommandArg {
    pub name: String,
    pub arg_type: CommandArgType,
    pub key_spec_index: Option<u32>,
    pub token: Option<String>,
    pub summary: Option<String>,
    pub since: Option<String>,
    pub flags: Option<Vec<CommandArgFlags>>,
    pub deprecated_since: Option<String>,
    pub subargs: Option<Vec<CommandArg>>,
    pub display_text: Option<String>,
}

impl Parse for CommandArg {
    fn parse(input: ParseStream) -> parse::Result<Self> {
        let content;
        braced!(content in input);
        let mut name = None;
        let mut arg_type = None;
        let mut key_spec_index = None;
        let mut token = None;
        let mut summary = None;
        let mut since = None;
        let mut flags = None;
        let mut deprecated_since = None;
        let mut subargs = None;
        let mut display_text = None;
        while !content.is_empty() {
            let key: Ident = content.parse()?;
            content.parse::<Token![:]>()?;
            match key.to_string().as_str() {
                "name" => {
                    let lit: LitStr = content.parse()?;
                    name = Some(lit.value());
                }
                "arg_type" => arg_type = Some(content.parse()?),
                "key_spec_index" => key_spec_index = Some(parse_u32(&content)?),
                "token" => {
                    let lit: LitStr = content.parse()?;
                    token = Some(lit.value());
                }
                "summary" => {
                    let lit: LitStr = content.parse()?;
                    summary = Some(lit.value());
                }
                "since" => {
                    let lit: LitStr = content.parse()?;
                    since = Some(lit.value());
                }
                "flags" => flags = Some(parse_bracket_list(&content)?),
                "deprecated_since" => {
                    let lit: LitStr = content.parse()?;
                    deprecated_since = Some(lit.value());
                }
                "subargs" => subargs = Some(parse_bracket_list(&content)?),
                "display_text" => {
                    let lit: LitStr = content.parse()?;
                    display_text = Some(lit.value());
                }
                other => return Err(syn::Error::new(key.span(), format!("unknown field `{other}`"))),
            }
            let _ = content.parse::<Token![,]>();
        }
        Ok(Self {
            name: name.ok_or_else(|| content.error("missing field `name`"))?,
            arg_type: arg_type.ok_or_else(|| content.error("missing field `arg_type`"))?,
            key_spec_index,
            token,
            summary,
            since,
            flags,
            deprecated_since,
            subargs,
            display_text,
        })
    }
}

fn parse_acl_category(input: ParseStream) -> parse::Result<AclCategory> {
    let ident: Ident = input.parse()?;
    match ident.to_string().as_str() {
        "None" => Ok(AclCategory::None),
        "Keyspace" => Ok(AclCategory::Keyspace),
        "Read" => Ok(AclCategory::Read),
        "Write" => Ok(AclCategory::Write),
        "Set" => Ok(AclCategory::Set),
        "SortedSet" => Ok(AclCategory::SortedSet),
        "List" => Ok(AclCategory::List),
        "Hash" => Ok(AclCategory::Hash),
        "String" => Ok(AclCategory::String),
        "Bitmap" => Ok(AclCategory::Bitmap),
        "HyperLogLog" => Ok(AclCategory::HyperLogLog),
        "Geo" => Ok(AclCategory::Geo),
        "Stream" => Ok(AclCategory::Stream),
        "PubSub" => Ok(AclCategory::PubSub),
        "Admin" => Ok(AclCategory::Admin),
        "Fast" => Ok(AclCategory::Fast),
        "Slow" => Ok(AclCategory::Slow),
        "Blocking" => Ok(AclCategory::Blocking),
        "Dangerous" => Ok(AclCategory::Dangerous),
        "Connection" => Ok(AclCategory::Connection),
        "Transaction" => Ok(AclCategory::Transaction),
        "Scripting" => Ok(AclCategory::Scripting),
        "Single" => {
            let inner;
            parenthesized!(inner in input);
            let lit: LitStr = inner.parse()?;
            Ok(AclCategory::Single(lit.value()))
        }
        other => Err(syn::Error::new(ident.span(), format!("unknown ACL category `{other}`"))),
    }
}

fn parse_acl_category_list(input: ParseStream) -> parse::Result<Vec<AclCategory>> {
    let content;
    bracketed!(content in input);
    let mut items = Vec::new();
    while !content.is_empty() {
        items.push(parse_acl_category(&content)?);
        let _ = content.parse::<Token![,]>();
    }
    Ok(items)
}

#[derive(Debug)]
struct Args {
    name: Option<String>,
    flags: Vec<RedisCommandFlags>,
    enterprise_flags: Option<Vec<RedisEnterpriseCommandFlags>>,
    summary: Option<String>,
    complexity: Option<String>,
    since: Option<String>,
    tips: Option<String>,
    arity: i64,
    key_spec: Vec<KeySpecArg>,
    args: Option<Vec<CommandArg>>,
    acl_categories: Option<Vec<AclCategory>>,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> parse::Result<Self> {
        let content;
        braced!(content in input);
        let mut name = None;
        let mut flags = None;
        let mut enterprise_flags = None;
        let mut summary = None;
        let mut complexity = None;
        let mut since = None;
        let mut tips = None;
        let mut arity = None;
        let mut key_spec = None;
        let mut args = None;
        let mut acl_categories = None;
        while !content.is_empty() {
            let key: Ident = content.parse()?;
            content.parse::<Token![:]>()?;
            match key.to_string().as_str() {
                "name" => {
                    let lit: LitStr = content.parse()?;
                    name = Some(lit.value());
                }
                "flags" => flags = Some(parse_bracket_list(&content)?),
                "enterprise_flags" => enterprise_flags = Some(parse_optional_bracket_list(&content)?),
                "summary" => {
                    let lit: LitStr = content.parse()?;
                    summary = Some(lit.value());
                }
                "complexity" => {
                    let lit: LitStr = content.parse()?;
                    complexity = Some(lit.value());
                }
                "since" => {
                    let lit: LitStr = content.parse()?;
                    since = Some(lit.value());
                }
                "tips" => {
                    let lit: LitStr = content.parse()?;
                    tips = Some(lit.value());
                }
                "arity" => arity = Some(parse_i64(&content)?),
                "key_spec" => key_spec = Some(parse_bracket_list(&content)?),
                "args" => args = Some(parse_bracket_list(&content)?),
                "acl_categories" => acl_categories = Some(parse_acl_category_list(&content)?),
                other => return Err(syn::Error::new(key.span(), format!("unknown field `{other}`"))),
            }
            let _ = content.parse::<Token![,]>();
        }
        Ok(Self {
            name,
            flags: flags.ok_or_else(|| content.error("missing field `flags`"))?,
            enterprise_flags,
            summary,
            complexity,
            since,
            tips,
            arity: arity.ok_or_else(|| content.error("missing field `arity`"))?,
            key_spec: key_spec.ok_or_else(|| content.error("missing field `key_spec`"))?,
            args,
            acl_categories,
        })
    }
}

fn to_token_stream(s: Option<String>) -> proc_macro2::TokenStream {
    s.map(|v| quote! {Some(#v.to_owned())})
        .unwrap_or(quote! {None})
}

fn generate_command_arg(arg: &CommandArg) -> proc_macro2::TokenStream {
    let name = &arg.name;
    let arg_type: u32 = arg.arg_type.into();
    let key_spec_index = arg
        .key_spec_index
        .map(|v| quote! {Some(#v)})
        .unwrap_or(quote! {None});
    let token = to_token_stream(arg.token.clone());
    let summary = to_token_stream(arg.summary.clone());
    let since = to_token_stream(arg.since.clone());
    let flags: Vec<&'static str> = arg
        .flags
        .as_ref()
        .map(|v| v.iter().map(|v| v.into()).collect())
        .unwrap_or_default();
    let flags = quote! {
        vec![#(redis_module::commands::CommandArgFlags::try_from(#flags)?, )*]
    };
    let deprecated_since = to_token_stream(arg.deprecated_since.clone());
    let display_text = to_token_stream(arg.display_text.clone());

    let subargs = if let Some(subargs_vec) = &arg.subargs {
        let subargs_tokens: Vec<_> = subargs_vec.iter().map(generate_command_arg).collect();
        quote! {
            Some(vec![#(#subargs_tokens),*])
        }
    } else {
        quote! { None }
    };

    quote! {
        redis_module::commands::RedisModuleCommandArg::new(
            #name.to_owned(),
            #arg_type,
            #key_spec_index,
            #token,
            #summary,
            #since,
            #flags.into(),
            #deprecated_since,
            #subargs,
            #display_text,
        )
    }
}

pub(crate) fn redis_command(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as Args);
    let func: ItemFn = match syn::parse(item) {
        Ok(res) => res,
        Err(e) => return e.to_compile_error().into(),
    };

    let original_function_name = func.sig.ident.clone();

    let c_function_name = Ident::new(&format!("_inner_{}", func.sig.ident), func.sig.ident.span());

    let get_command_info_function_name = Ident::new(
        &format!("_inner_get_command_info_{}", func.sig.ident),
        func.sig.ident.span(),
    );

    let name_literal = args
        .name
        .unwrap_or_else(|| original_function_name.to_string());
    let flags_str = args
        .flags
        .into_iter()
        .fold(String::new(), |s, v| {
            format!("{} {}", s, Into::<&'static str>::into(&v))
        })
        .trim()
        .to_owned();
    let flags_literal = quote!(#flags_str);
    let enterprise_flags_str = args
        .enterprise_flags
        .map(|v| {
            v.into_iter()
                .fold(String::new(), |s, v| {
                    format!("{} {}", s, Into::<&'static str>::into(&v))
                })
                .trim()
                .to_owned()
        })
        .unwrap_or_default();

    let enterprise_flags_literal = quote!(#enterprise_flags_str);
    let summary_literal = to_token_stream(args.summary);
    let complexity_literal = to_token_stream(args.complexity);
    let since_literal = to_token_stream(args.since);
    let tips_literal = to_token_stream(args.tips);
    let arity_literal = args.arity;
    let key_spec_notes: Vec<_> = args
        .key_spec
        .iter()
        .map(|v| {
            v.notes
                .as_ref()
                .map(|v| quote! {Some(#v.to_owned())})
                .unwrap_or(quote! {None})
        })
        .collect();

    let key_spec_flags: Vec<_> = args
        .key_spec
        .iter()
        .map(|v| {
            let flags: Vec<&'static str> = v.flags.iter().map(|v| v.into()).collect();
            quote! {
                vec![#(redis_module::commands::KeySpecFlags::try_from(#flags)?, )*]
            }
        })
        .collect();

    let key_spec_begin_search: Vec<_> = args
        .key_spec
        .iter()
        .map(|v| match &v.begin_search {
            BeginSearch::Index(i) => {
                let i = i.index;
                quote! {
                    redis_module::commands::BeginSearch::new_index(#i)
                }
            }
            BeginSearch::Keyword(begin_search_keyword) => {
                let k = begin_search_keyword.keyword.as_str();
                let i = begin_search_keyword.startfrom;
                quote! {
                    redis_module::commands::BeginSearch::new_keyword(#k.to_owned(), #i)
                }
            }
        })
        .collect();

    let key_spec_find_keys: Vec<_> = args
        .key_spec
        .iter()
        .map(|v| match &v.find_keys {
            FindKeys::Keynum(find_keys_num) => {
                let keynumidx = find_keys_num.key_num_idx;
                let firstkey = find_keys_num.first_key;
                let keystep = find_keys_num.key_step;
                quote! {
                    redis_module::commands::FindKeys::new_keys_num(#keynumidx, #firstkey, #keystep)
                }
            }
            FindKeys::Range(find_keys_range) => {
                let last_key = find_keys_range.last_key;
                let steps = find_keys_range.steps;
                let limit = find_keys_range.limit;
                quote! {
                    redis_module::commands::FindKeys::new_range(#last_key, #steps, #limit)
                }
            }
        })
        .collect();

    let command_args: Vec<_> = args
        .args
        .as_ref()
        .map(|v| v.iter().map(generate_command_arg).collect())
        .unwrap_or_default();

    let acl_categories = args
        .acl_categories
        .map(|v| v.into_iter().map(String::from).collect::<Vec<_>>());

    let acl_categories_tokens = if let Some(categories) = &acl_categories {
        quote! {
            Some(vec![#(#categories.to_owned()),*])
        }
    } else {
        quote! { None }
    };

    let gen = quote! {
        #func

        extern "C" fn #c_function_name(
            ctx: *mut redis_module::raw::RedisModuleCtx,
            argv: *mut *mut redis_module::raw::RedisModuleString,
            argc: i32,
        ) -> i32 {
            let context = redis_module::Context::new(ctx);

            let args = redis_module::decode_args(ctx, argv, argc);
            let response = #original_function_name(&context, args);
            context.reply(response.map(|v| v.into())) as i32
        }

        #[linkme::distributed_slice(redis_module::commands::COMMANDS_LIST)]
        fn #get_command_info_function_name() -> Result<redis_module::commands::CommandInfo, redis_module::RedisError> {
            let key_spec = vec![
                #(
                    redis_module::commands::KeySpec::new(
                        #key_spec_notes,
                        #key_spec_flags.into(),
                        #key_spec_begin_search,
                        #key_spec_find_keys,
                    ),
                )*
            ];
            let command_args = vec![#(#command_args),*];
            Ok(redis_module::commands::CommandInfo::new(
                #name_literal.to_owned(),
                Some(#flags_literal.to_owned()),
                Some(#enterprise_flags_literal.to_owned()),
                #summary_literal,
                #complexity_literal,
                #since_literal,
                #tips_literal,
                #arity_literal,
                key_spec,
                #c_function_name,
                command_args,
                #acl_categories_tokens,
            ))
        }
    };
    gen.into()
}
