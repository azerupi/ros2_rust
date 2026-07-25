//! Parsing `#[parameters(...)]` and `#[param(...)]`.

use syn::{spanned::Spanned, Attribute, Expr, ExprRange, Ident, LitStr, RangeLimits};

use crate::errors::Errors;

/// Struct-level configuration from `#[parameters(...)]`.
#[derive(Default)]
pub(crate) struct SetAttrs {
    /// Namespace to declare this set under at the top level. `None` means the node's root.
    pub namespace: Option<LitStr>,
    /// Expression of type `Self` supplying the default value of every field that has none.
    pub default: Option<Expr>,
    /// Name for the generated handles struct.
    pub handles: Option<Ident>,
}

impl SetAttrs {
    pub fn parse(attrs: &[Attribute], errors: &mut Errors) -> Self {
        let mut parsed = Self::default();
        for attr in attrs {
            if !attr.path().is_ident("parameters") {
                continue;
            }
            let result = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("namespace") {
                    parsed.namespace = Some(meta.value()?.parse()?);
                } else if meta.path.is_ident("default") {
                    parsed.default = Some(meta.value()?.parse()?);
                } else if meta.path.is_ident("handles") {
                    parsed.handles = Some(meta.value()?.parse()?);
                } else {
                    return Err(meta.error(format!(
                        "unknown `parameters` option `{}`; expected one of `namespace`, \
                         `default`, `handles`",
                        path_name(&meta.path),
                    )));
                }
                Ok(())
            });
            errors.handle(result);
        }
        parsed
    }
}

/// Field-level configuration from `#[param(...)]`, plus the field's doc comment.
#[derive(Default)]
pub(crate) struct FieldAttrs {
    pub default: Option<Expr>,
    pub description: Option<LitStr>,
    pub constraints: Option<LitStr>,
    pub range: Option<ExprRange>,
    pub step: Option<Expr>,
    pub read_only: Option<Ident>,
    pub ignore_override: Option<Ident>,
    pub discard_mismatching_prior_value: Option<Ident>,
    pub validate: Option<Expr>,
    pub on_change: Option<Expr>,
    pub discriminate: Option<Expr>,
    pub rename: Option<LitStr>,
    pub flatten: Option<Ident>,
    pub skip: Option<Ident>,
    /// The field's doc comment, used as the description when none is given explicitly.
    pub doc: Option<String>,
}

impl FieldAttrs {
    pub fn parse(attrs: &[Attribute], errors: &mut Errors) -> Self {
        let mut parsed = Self {
            doc: doc_comment(attrs),
            ..Default::default()
        };

        for attr in attrs {
            if !attr.path().is_ident("param") {
                continue;
            }
            let result =
                attr.parse_nested_meta(|meta| {
                    let name = path_name(&meta.path);
                    let flag = || -> Ident {
                        meta.path
                            .get_ident()
                            .cloned()
                            .unwrap_or_else(|| Ident::new("param", meta.path.span()))
                    };
                    match name.as_str() {
                        "default" => parsed.default = Some(meta.value()?.parse()?),
                        "description" => parsed.description = Some(meta.value()?.parse()?),
                        "constraints" => parsed.constraints = Some(meta.value()?.parse()?),
                        "range" => {
                            let value: Expr = meta.value()?.parse()?;
                            match value {
                                Expr::Range(range) => parsed.range = Some(range),
                                other => return Err(syn::Error::new(
                                    other.span(),
                                    "`range` takes a Rust range, such as `0.0..=10.0`, `0..` or \
                                     `..=100`",
                                )),
                            }
                        }
                        "step" => parsed.step = Some(meta.value()?.parse()?),
                        "read_only" => parsed.read_only = Some(flag()),
                        "ignore_override" => parsed.ignore_override = Some(flag()),
                        "discard_mismatching_prior_value" => {
                            parsed.discard_mismatching_prior_value = Some(flag())
                        }
                        "validate" => parsed.validate = Some(meta.value()?.parse()?),
                        "on_change" => parsed.on_change = Some(meta.value()?.parse()?),
                        "discriminate" => parsed.discriminate = Some(meta.value()?.parse()?),
                        "rename" => parsed.rename = Some(meta.value()?.parse()?),
                        "flatten" => parsed.flatten = Some(flag()),
                        "skip" => parsed.skip = Some(flag()),
                        "mandatory" | "optional" => {
                            return Err(meta.error(format!(
                            "`{name}` is not needed: a field is an optional parameter when its \
                             type is an `Option`, and a mandatory one otherwise"
                        )))
                        }
                        "nested" => {
                            return Err(meta.error(
                                "`nested` is not needed: a field whose type is a parameter set is \
                             declared as one automatically",
                            ))
                        }
                        _ => {
                            return Err(meta.error(format!(
                                "unknown `param` option `{name}`; expected one of `default`, \
                             `description`, `constraints`, `range`, `step`, `read_only`, \
                             `ignore_override`, `discard_mismatching_prior_value`, `validate`, \
                             `on_change`, `discriminate`, `rename`, `flatten`, `skip`"
                            )))
                        }
                    }
                    Ok(())
                });
            errors.handle(result);
        }

        parsed
    }

    /// The description to put in the parameter descriptor.
    pub fn description_text(&self) -> String {
        match &self.description {
            Some(explicit) => explicit.value(),
            None => self.doc.clone().unwrap_or_default(),
        }
    }

    /// The span of any option other than `skip` itself, for reporting attributes that `skip`
    /// makes meaningless.
    pub fn conflicts_with_skip(&self) -> Option<proc_macro2::Span> {
        macro_rules! first_of {
            ($($field:ident),*) => {
                $(if let Some(value) = &self.$field {
                    return Some(value.span());
                })*
            };
        }
        first_of!(
            default,
            description,
            constraints,
            range,
            step,
            read_only,
            ignore_override,
            discard_mismatching_prior_value,
            validate,
            on_change,
            discriminate,
            rename,
            flatten
        );
        None
    }
}

/// Extracts the text of a doc comment, with the leading space of each line removed.
fn doc_comment(attrs: &[Attribute]) -> Option<String> {
    let mut lines = Vec::new();
    for attr in attrs {
        if !attr.path().is_ident("doc") {
            continue;
        }
        let syn::Meta::NameValue(name_value) = &attr.meta else {
            continue;
        };
        let Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(text),
            ..
        }) = &name_value.value
        else {
            continue;
        };
        let line = text.value();
        lines.push(line.strip_prefix(' ').unwrap_or(&line).to_string());
    }
    if lines.is_empty() {
        return None;
    }
    // Trailing blank lines carry no meaning in a descriptor.
    while lines.last().is_some_and(|line| line.trim().is_empty()) {
        lines.pop();
    }
    Some(lines.join("\n"))
}

/// The bounds of a `#[param(range = ...)]`, as expressions.
pub(crate) struct RangeBounds<'a> {
    pub lower: Option<&'a Expr>,
    pub upper: Option<&'a Expr>,
}

/// Interprets a Rust range as parameter range bounds.
///
/// Only inclusive and open-ended ranges are accepted. ROS 2 parameter ranges are inclusive, and
/// an exclusive upper bound has no representation in one: it would have to be rewritten as
/// `upper - 1`, which is wrong for floating point and surprising for integers.
pub(crate) fn range_bounds(range: &ExprRange) -> syn::Result<RangeBounds<'_>> {
    if matches!(range.limits, RangeLimits::HalfOpen(_)) && range.end.is_some() {
        return Err(syn::Error::new(
            range.span(),
            "a parameter range is inclusive of its bounds, so an exclusive range cannot be used \
             here; write `..=` instead of `..`",
        ));
    }
    if range.start.is_none() && range.end.is_none() {
        return Err(syn::Error::new(
            range.span(),
            "`..` places no bounds on the parameter, so the `range` can be removed",
        ));
    }
    Ok(RangeBounds {
        lower: range.start.as_deref(),
        upper: range.end.as_deref(),
    })
}

fn path_name(path: &syn::Path) -> String {
    path.get_ident()
        .map(|ident| ident.to_string())
        .unwrap_or_else(|| quote::quote!(#path).to_string())
}
