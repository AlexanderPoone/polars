use std::io::Cursor;

use polars::prelude::*;
use polars_excel_writer::PolarsXlsxWriter;

fn main() -> PolarsResult<()> {
    let data = r#"[
        {"date": "1996-12-16T00:00:00.000", "open": 16.86, "close": 16.86, "high": 16.86, "low": 16.86, "volume": 62442.0, "turnover": 105277000.0},
        {"date": "1996-12-17T00:00:00.000", "open": 15.17, "close": 15.17, "high": 16.79, "low": 15.17, "volume": 463675.0, "turnover": 718902016.0},
        {"date": "1996-12-18T00:00:00.000", "open": 15.28, "close": 16.69, "high": 16.69, "low": 15.18, "volume": 445380.0, "turnover": 719400000.0},
        {"date": "1996-12-19T00:00:00.000", "open": 17.01, "close": 16.4, "high": 17.9, "low": 15.99, "volume": 572946.0, "turnover": 970124992.0}
    ]"#;

    let res = JsonReader::new(Cursor::new(data)).finish();
    println!("{:?}", res);    // <-------------------------------------------- both wrapped and unwrapped versions can be printed
    assert!(res.is_ok());
    let df = res.unwrap();
    println!("{:?}", df);    // <--------------------------------------------- both wrapped and unwrapped versions can be printed
    let mut xlsx_writer = PolarsXlsxWriter::new();

    xlsx_writer.write_dataframe(&df)?;
    xlsx_writer.save("../datasets/out1.xlsx")?;

    

    /////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////
    let df = LazyFrame::scan_parquet("../datasets/foods1.parquet", ScanArgsParquet::default())?
        .select([
            // select all columns
            all(),
            // and do some aggregations
            cols(["fats_g", "sugars_g"]).sum().name().suffix("_summed"),
        ])
        .collect()?;

    println!("{}", df);
    Ok(())
}
