//! Implementation of `#[derive(ParameterSet)]`.
//!
//! The macro emits the same shape of code for every field of the struct:
//!
//! ```ignore
//! field: <FieldType as DeclareField<Mode>>::declare(node, &name, spec)?
//! ```
//!
//! It therefore does not need to know what kind of parameter a field is. Whether a field is a
//! single parameter, an optional one, or a whole nested set of parameters is decided by which
//! `DeclareField` implementation applies to its type, which is what allows a nested set to be
//! written without any annotation and a user-defined type to be used as a parameter without the
//! macro knowing about it.
//!
//! The macro does inspect field types, but only to produce better diagnostics. See
//! [`known_types`].

mod attrs;
mod codegen;
mod enum_set;
mod known_types;

use proc_macro2::TokenStream;
use syn::{spanned::Spanned, Data, DeriveInput, Fields, Ident};

use crate::errors::Errors;
use attrs::{FieldAttrs, SetAttrs};
use known_types::{shape_of, TypeShape};

/// The parameter name an identifier stands for.
///
/// A raw identifier is written `r#type` in Rust but names the parameter `type`, which is the
/// point of using one: it lets a field be called after a parameter whose name is a keyword.
pub(crate) fn parameter_name(ident: &Ident) -> String {
    ident.to_string().trim_start_matches("r#").to_string()
}

/// One field of the struct, with everything needed to generate its declaration.
pub(crate) struct Field<'a> {
    pub ident: &'a Ident,
    pub ty: &'a syn::Type,
    /// The parameter name, which is the field name unless `rename` says otherwise.
    pub name: String,
    pub attrs: FieldAttrs,
}

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream> {
    let mut errors = Errors::default();

    let set_attrs = SetAttrs::parse(&input.attrs, &mut errors);

    if !input.generics.params.is_empty() {
        errors.at(
            &input.generics,
            "`ParameterSet` cannot be derived for a generic type: the parameters to declare have \
             to be known when the type is defined",
        );
    }

    let generated = match &input.data {
        Data::Struct(data) => expand_struct(input, &set_attrs, data, &mut errors),
        Data::Enum(data) => expand_enum(input, &set_attrs, data, &mut errors),
        Data::Union(data) => {
            errors.at(
                data.union_token,
                "`ParameterSet` cannot be derived for a union",
            );
            None
        }
    };

    // Generating code from a type that was rejected only produces a second round of errors about
    // the code that was generated from it.
    errors.into_result()?;
    Ok(generated.expect("code was generated when no error was recorded"))
}

/// A struct: every field is a parameter.
fn expand_struct(
    input: &DeriveInput,
    set_attrs: &SetAttrs,
    data: &syn::DataStruct,
    errors: &mut Errors,
) -> Option<TokenStream> {
    if let Some(tag) = &set_attrs.tag {
        errors.at(
            tag,
            "`tag` names the parameter that says which variant of an enum is in use, so it has \
             no meaning for a struct",
        );
    }
    if let Some((_, span)) = &set_attrs.rename_all {
        errors.at(
            span,
            "`rename_all` names the variants of an enum, so it has no meaning for a struct. Use \
             `#[param(rename = \"...\")]` to rename an individual parameter",
        );
    }

    let named_fields = match &data.fields {
        Fields::Named(fields) => &fields.named,
        Fields::Unnamed(_) => {
            errors.at(
                &data.fields,
                "`ParameterSet` requires named fields, because each field's name is the name of \
                 the parameter it declares",
            );
            return None;
        }
        Fields::Unit => {
            errors.at(
                &input.ident,
                "`ParameterSet` requires a struct with at least one field",
            );
            return None;
        }
    };

    let mut fields = Vec::new();
    for field in named_fields {
        let ident = field
            .ident
            .as_ref()
            .expect("fields of a named struct have idents");
        let attrs = FieldAttrs::parse(&field.attrs, errors);
        check_field(field, &attrs, errors);
        let name = match &attrs.rename {
            Some(rename) => rename.value(),
            None => parameter_name(ident),
        };
        fields.push(Field {
            ident,
            ty: &field.ty,
            name,
            attrs,
        });
    }

    if let Some(duplicate) = duplicate_parameter_name(&fields) {
        errors.at(
            duplicate.ident,
            format!(
                "another field of this set already declares a parameter called `{}`",
                duplicate.name
            ),
        );
    }

    if !errors.is_empty() {
        return None;
    }
    Some(codegen::generate(input, set_attrs, &fields))
}

/// An enum: a read-only tag parameter says which variant is in use, and that variant's
/// parameters are declared alongside it.
fn expand_enum(
    input: &DeriveInput,
    set_attrs: &SetAttrs,
    data: &syn::DataEnum,
    errors: &mut Errors,
) -> Option<TokenStream> {
    let tag = set_attrs
        .tag
        .as_ref()
        .map(|tag| tag.value())
        .unwrap_or_else(|| enum_set::DEFAULT_TAG.to_string());

    let variants = enum_set::variants(
        data,
        &input.ident,
        set_attrs
            .rename_all
            .as_ref()
            .map(|(convention, _)| *convention),
        &tag,
        errors,
    );

    if !errors.is_empty() {
        return None;
    }
    Some(codegen::generate_enum(input, set_attrs, &tag, &variants))
}

/// Reports the mistakes the macro can recognise from the field's type and attributes.
pub(crate) fn check_field(field: &syn::Field, attrs: &FieldAttrs, errors: &mut Errors) {
    let shape = shape_of(&field.ty);

    if attrs.skip.is_some() {
        if let Some(conflict) = attrs.conflicts_with_skip() {
            errors.push(syn::Error::new(
                conflict,
                "this option has no effect on a `skip`ped field, which is not a parameter at all",
            ));
        }
        // A skipped field is not a parameter, so nothing else about it matters.
        return;
    }

    if let TypeShape::Rejected(message) = &shape {
        errors.at(&field.ty, message);
        return;
    }

    if let Some(read_only) = &attrs.read_only {
        if shape.is_optional() {
            errors.at(
                read_only,
                "`read_only` cannot be combined with `Option`: a read-only parameter always has \
                 a value, so there is nothing for the absence of one to mean. Remove `read_only`, \
                 or drop the `Option`",
            );
        }
        if let Some(on_change) = &attrs.on_change {
            errors.at(
                on_change,
                "`on_change` cannot be used on a `read_only` parameter, which never changes",
            );
        }
        if let Some(validate) = &attrs.validate {
            errors.at(
                validate,
                "`validate` on a `read_only` parameter only ever runs once, on the initial \
                 value; if that is what you want, say so with `discriminate` instead",
            );
        }
    }

    if let Some(range) = &attrs.range {
        if !shape.accepts_range() {
            errors.at(range, shape.range_rejection());
        }
        if let Err(error) = attrs::range_bounds(range) {
            errors.push(error);
        }
    } else if let Some(step) = &attrs.step {
        errors.at(
            step,
            "`step` describes the values within a range, so it needs a `range` to go with it",
        );
    }

    if let Some(flatten) = &attrs.flatten {
        if shape.is_definitely_leaf() {
            errors.at(
                flatten,
                "`flatten` declares the fields of a nested parameter set directly under this \
                 set's namespace, so it applies only to fields whose type is a parameter set",
            );
        }
        if let Some(rename) = &attrs.rename {
            errors.at(
                rename,
                "a `flatten`ed set has no name of its own, so it cannot be renamed",
            );
        }
    }
}

/// Two fields declaring the same parameter name, which `rename` makes possible.
pub(crate) fn duplicate_parameter_name<'a>(fields: &'a [Field<'a>]) -> Option<&'a Field<'a>> {
    let mut seen = std::collections::HashSet::new();
    fields
        .iter()
        .filter(|field| field.attrs.skip.is_none() && field.attrs.flatten.is_none())
        .find(|field| !seen.insert(field.name.clone()))
}

impl Field<'_> {
    /// Whether this field declares a parameter at all.
    pub fn is_declared(&self) -> bool {
        self.attrs.skip.is_none()
    }

    /// The mode marker this field is declared in.
    pub fn mode(&self) -> TokenStream {
        if self.attrs.read_only.is_some() {
            quote::quote!(::rclrs::ReadOnly)
        } else {
            quote::quote!(::rclrs::Writable)
        }
    }

    pub fn span(&self) -> proc_macro2::Span {
        self.ty.span()
    }
}

#[cfg(test)]
#[path = "expand_tests.rs"]
mod expand_tests;
