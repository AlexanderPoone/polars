use std::path::PathBuf;

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
        .with_has_header(true)                             // default is true
        .with_n_rows(Some(10))
        .with_columns(Some(                                              // `!vec` is not needed here
            [
                PlSmallStr::from_str("category"),                        // Good optimisation via `PlSmallStr`
                PlSmallStr::from_str("calories"),
            ]
            .into(),
        ))
        // .with_skip_rows_after_header(10)
        .with_rechunk(true)
        .with_chunk_size(10)
        .with_parse_options(
            CsvParseOptions::default()                   // Delimiter is under `CsvParseOptions`
                .with_separator(b' ')
                .with_try_parse_dates(true)
                .with_null_values(Some(NullValues::Named(vec![(         // `!vec` is needed here
                    PlSmallStr::from_str("category"),
                    PlSmallStr::from_str("vegetables"),
                ), (
                    PlSmallStr::from_str("calories"),
                    PlSmallStr::from_str("-1"),
                )]))),
        )
        .try_into_reader_with_file_path(Some(PathBuf::from("../datasets/foods4.csv")))?
        .finish()?
        .drop_nulls::<String>(None)?;                            // <String> is needed here
    println!("{_dff:?}");

    // write_other_formats(&mut df)?;
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
