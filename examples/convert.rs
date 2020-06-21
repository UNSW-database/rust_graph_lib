use indexmap::set::IndexSet;

// Give a text file of *ID degree* sorted by degree
// and a text file of edges in the from of *src dst*,
// reorder the graph by the degree of nodes.
fn main() {
    // TODO: Read the first file, put ids in an IndexSet if degree!=0
    let degree_file = std::env::args().nth(1).unwrap();
    let edge_file = std::env::args().nth(2).unwrap();
    let output_file = std::env::args().nth(3).unwrap();

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(degree_file)
        .expect("Cannot read field");

    let mut map = IndexSet::new();

    for result in reader.records() {
        let record = result.unwrap();
        let id = record[0].trim().parse::<u32>().unwrap();
        let degree = record[1].trim().parse::<u32>().unwrap();

        if degree != 0 {
            map.insert(id);
        }
    }

    // TODO: Read the second file, convert each id using the above mapping and write to a new file
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(edge_file)
        .expect("Cannot read field");

    let mut wtr = csv::Writer::from_path(output_file).unwrap();

    for result in reader.records() {
        let record = result.unwrap();
        let src = record[0].trim().parse::<u32>().unwrap();
        let dst = record[1].trim().parse::<u32>().unwrap();

        let edge = vec![
            map.get_full(&src).unwrap().0.to_string(),
            map.get_full(&dst).unwrap().0.to_string(),
        ];
        wtr.write_record(&edge).unwrap();
    }

    wtr.flush().unwrap();
}
