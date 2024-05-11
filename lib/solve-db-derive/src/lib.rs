use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DataEnum, DataStruct, DeriveInput, Fields, FieldsNamed};

#[proc_macro_derive(FromRow)]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(FieldsNamed { named, .. }),
            ..
        }) => {
            let ident = &input.ident;
            let tokens = named.iter().map(|field| {
                let name = &field.ident;
                let name_str = quote!(#name).to_string();
                quote! {
                    #name: row.get_parsed(#name_str)?
                }
            });
            return TokenStream::from(quote! {
                impl FromRow for #ident {
                    fn from_row(row: &solve_db::Row) -> Result<Self, solve_db::Error> {
                        Ok(Self { #(#tokens),* })
                    }
                }
            });
        }
        _ => TokenStream::from(
            syn::Error::new_spanned(input, "Only structs with named fields can derive `FromRow`")
                .to_compile_error(),
        ),
    }
}

#[proc_macro_derive(IntoRow)]
pub fn derive_into_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(FieldsNamed { named, .. }),
            ..
        }) => {
            let ident = &input.ident;
            let tokens = named.iter().map(|field| {
                let name = &field.ident;
                let name_str = quote!(#name).to_string();
                quote! {
                    (#name_str.into(), solve_db::IntoValue::into_value(self.#name))
                }
            });
            return TokenStream::from(quote! {
                impl IntoRow for #ident {
                    fn into_row(self) -> solve_db::SimpleRow {
                        vec![ #(#tokens),* ]
                    }
                }
            });
        }
        _ => TokenStream::from(
            syn::Error::new_spanned(input, "Only structs with named fields can derive `IntoRow`")
                .to_compile_error(),
        ),
    }
}

#[proc_macro_derive(FromValue)]
pub fn derive_from_value(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match &input.data {
        Data::Enum(DataEnum { variants, .. }) => {
            let ident = &input.ident;
            let from_tokens = variants.iter().map(|variant| {
                let name = &variant.ident;
                match &variant.discriminant {
                    Some((_, discriminant)) => quote! {#discriminant => Self::#name},
                    None => quote! {v => Self::#name(v.try_into()?)},
                }
            });
            return TokenStream::from(quote! {
                impl solve_db::FromValue for #ident {
                    fn from_value(value: &solve_db::Value) -> Result<Self, solve_db::Error> {
                        Ok(match value.parse()? { #(#from_tokens),* })
                    }
                }
            });
        }
        _ => TokenStream::from(
            syn::Error::new_spanned(input, "Only enums can derive `FromValue`").to_compile_error(),
        ),
    }
}

#[proc_macro_derive(IntoValue)]
pub fn derive_into_value(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match &input.data {
        Data::Enum(DataEnum { variants, .. }) => {
            let ident = &input.ident;
            let into_tokens = variants.iter().map(|variant| {
                let name = &variant.ident;
                match &variant.discriminant {
                    Some((_, discriminant)) => quote! {#ident::#name => #discriminant},
                    None => quote! {#ident::#name(v) => v.into()},
                }
            });
            return TokenStream::from(quote! {
                impl solve_db::IntoValue for #ident {
                    fn into_value(self) -> solve_db::Value {
                        solve_db::Value::from(match self { #(#into_tokens),* })
                    }
                }
            });
        }
        _ => TokenStream::from(
            syn::Error::new_spanned(input, "Only enums can derive `FromValue`").to_compile_error(),
        ),
    }
}

#[proc_macro_derive(Value)]
pub fn derive_value(input: TokenStream) -> TokenStream {
    let mut output = derive_from_value(input.clone());
    output.extend(derive_into_value(input));
    output
}
