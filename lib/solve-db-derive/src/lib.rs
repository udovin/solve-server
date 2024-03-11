use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DataStruct, DeriveInput, Fields, FieldsNamed};

#[proc_macro_derive(FromRow)]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(FieldsNamed { named, .. }),
            ..
        }) => {
            let ident = &input.ident;
            let field_vals = named.iter().map(|field| {
                let name = &field.ident;
                let name_str = quote!(#name).to_string();
                quote!{
                    #name: row.get(#name_str)?.try_into()?
                }
            });
            return TokenStream::from(quote!{
                impl FromRow for #ident {
                    fn from_row(row: &Row) -> Result<Self, Error> {
                        Ok(Self { #(#field_vals),* })
                    }
                }
            });
        }
        _ => TokenStream::from(syn::Error::new_spanned(
            input,
            "Only structs with named fields can derive `FromRow`",
        ).to_compile_error()),
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
            let field_vals = named.iter().map(|field| {
                let name = &field.ident;
                let name_str = quote!(#name).to_string();
                quote!{
                    (#name_str.into(), self.#name.into())
                }
            });
            return TokenStream::from(quote!{
                impl IntoRow for #ident {
                    fn into_row(self) -> Vec<(String, Value)> {
                        vec![ #(#field_vals),* ]
                    }
                }
            });
        }
        _ => TokenStream::from(syn::Error::new_spanned(
            input,
            "Only structs with named fields can derive `IntoRow`",
        ).to_compile_error()),
    }
}
