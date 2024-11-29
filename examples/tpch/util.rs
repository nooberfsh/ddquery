use std::fs::{read_to_string, File};
use std::io::Read;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context, Error, Result};
use rayon::prelude::*;

pub fn parse_field<'a, T: FromStr>(
    fields: &mut impl Iterator<Item = &'a str>,
) -> Result<T, <T as FromStr>::Err> {
    fields.next().unwrap_or("").parse()
}

pub fn parse_field_trim<'a, T: FromStr>(
    fields: &mut impl Iterator<Item = &'a str>,
) -> Result<T, <T as FromStr>::Err> {
    fields.next().unwrap_or("").trim().parse()
}

pub fn load_output<T>(path: &str, name: &str) -> Result<Vec<T>>
where
    T: FromStr<Err = Error>,
{
    let mut path = PathBuf::from(path);
    path.push(name);
    let data =
        read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut ret = vec![];
    for (num, line) in data.lines().enumerate().skip(1) {
        let d = line
            .parse()
            .with_context(|| format!("failed to parse {}, line[{}]: {}", name, num + 1, line))?;
        ret.push(d);
    }
    Ok(ret)
}

pub fn load_input<T>(path: &str, name: &str, batch_size: usize) -> Result<Vec<Vec<T>>>
where
    T: FromStr<Err = Error> + Send + 'static,
{
    let timer = std::time::Instant::now();

    let mut path = PathBuf::from(path);
    path.push(name);

    let mut items_file =
        File::open(&path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut data = String::new();
    items_file.read_to_string(&mut data).unwrap();

    let t_read_file = timer.elapsed();

    let lines: Vec<_> = data.lines().collect();
    let t_collect_lines = timer.elapsed();

    let count = lines.len();

    let res = lines
        .into_par_iter()
        // TODO: we should avoid unwrap
        .map(|s| T::from_str(s).unwrap())
        .chunks(batch_size)
        .collect();
    let t_parse = timer.elapsed();

    println!(
        "load finish, read_file: {:?}, collect_lines: {:?}, parse: {:?}, total: {:?}, count: {}",
        t_read_file,
        t_collect_lines - t_read_file,
        t_parse - t_collect_lines,
        t_parse,
        count
    );

    Ok(res)
}
