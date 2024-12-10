use std::path::PathBuf;

use calamine::{open_workbook, Data, Reader, Xlsx};
use polars::io::mmap::MmapBytesReader;
use polars::prelude::*;

fn main() -> PolarsResult<()> {
    let file = std::fs::File::open("../datasets/foods1.csv").unwrap();
    let file = Box::new(file) as Box<dyn MmapBytesReader>;
    let _df = CsvReader::new(file)
        // .with_separator(b'|')
        // .has_header(false)
        // .with_chunk_size(10)
        .batched(None)
        .unwrap();

    let _dff = CsvReadOptions::default()
        .with_has_header(true) // default is true
        .with_n_rows(Some(10))
        .with_columns(Some(
            // `!vec` is not needed here
            [
                PlSmallStr::from_str("category"), // Good optimisation via `PlSmallStr`
                PlSmallStr::from_str("calories"),
            ]
            .into(),
        ))
        // .with_skip_rows_after_header(10)
        .with_rechunk(true)
        .with_chunk_size(1)
        .with_parse_options(
            CsvParseOptions::default() // Delimiter is under `CsvParseOptions`
                .with_separator(b' ')
                .with_try_parse_dates(true)
                .with_null_values(Some(NullValues::Named(vec![
                    (
                        // `!vec` is needed here
                        PlSmallStr::from_str("category"),
                        PlSmallStr::from_str("vegetables"),
                    ),
                    (PlSmallStr::from_str("calories"), PlSmallStr::from_str("-1")),
                ]))),
        )
        .try_into_reader_with_file_path(Some(PathBuf::from("../datasets/foods4.csv")))?
        .finish()?
        .drop_nulls::<String>(None)? // <String> is needed here
        .with_row_index("Id".into(), None)?;

    // cf. iterrows() BELOW
    let mut row = _dff.get_row(0)?; // pass row by reference, mutable
    let col = _dff.get_column_index("category").unwrap(); //  the `?` operator can only be used on `Result`s, not `Option`s, in a function that returns `Result`
    let mut category = row.0[col].get_str().unwrap(); // .0 turns the pl::Row object into a Vec
    println!("{row:?}          {category:?}");
    for idx in 1.._dff.height() {
        let _ = _dff.get_row_amortized(idx, &mut row); // pass row by reference,
        category = row.0[col].get_str().unwrap(); // .0 turns the pl::Row object into a Vec

        if category == "meat" {
            // to compare strings, we must use &str NOT String
            break;
        }
        println!("{row:?}          {category:?}");
    }
    // cf. iterrows() ABOVE
    println!("{_dff:?}");

    let mut workbook: Xlsx<_> = open_workbook("../datasets/foods1.xlsx").expect("Cannot open file");
    let sheet = workbook.worksheet_range("Sheet1").unwrap();

    Ok(())
}

// fn _write_other_formats(df: &mut DataFrame) -> PolarsResult<()> {
//     let parquet_out = "../datasets/foods1.parquet";
//     if std::fs::metadata(parquet_out).is_err() {
//         let f = std::fs::File::create(parquet_out).unwrap();
//         ParquetWriter::new(f).with_statistics(true).finish(df)?;
//     }
//     let ipc_out = "../datasets/foods1.ipc";
//     if std::fs::metadata(ipc_out).is_err() {
//         let f = std::fs::File::create(ipc_out).unwrap();
//         IpcWriter::new(f).finish(df)?
//     }
//     Ok(())
// }
