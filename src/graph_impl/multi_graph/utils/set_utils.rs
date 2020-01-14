use std::iter::FromIterator;

pub fn get_power_set_excluding_empty_set<T: Clone>(set: Vec<T>) -> Vec<Vec<T>> {
    let mut res = vec![];
    let len = set.len();
    for sub in generate_power_set(set) {
        if sub.len() >= 1 && sub.len() <= len {
            res.push(sub);
        }
    }
    res
}

pub fn generate_permutations<T: Clone>(mut set: Vec<T>, len: usize) -> Vec<Vec<T>> {
    let mut permutations = vec![];
    get_permutations_given_len(&mut set, len, 0, &mut vec![], &mut permutations);
    permutations
}

fn get_permutations_given_len<T: Clone>(
    set: &mut Vec<T>,
    len: usize,
    pos: usize,
    temp: &mut Vec<T>,
    permutation: &mut Vec<Vec<T>>,
) {
    if len == 0 {
        permutation.push(temp.clone());
        return;
    }
    for i in 0..set.len() {
        if temp.len() < pos + 1 {
            temp.push(set[i].clone());
        } else {
            temp.insert(pos, set[i].clone());
        }
        get_permutations_given_len(set, len - 1, pos + 1, temp, permutation);
    }
}

fn generate_power_set<T: Clone>(original_set: Vec<T>) -> Vec<Vec<T>> {
    let mut sets = vec![];
    if original_set.is_empty() {
        sets.push(vec![]);
        return sets;
    }
    let list = original_set.clone();
    let head = list.get(0).unwrap();
    let rest = list[1..list.len()].iter().map(|x| x.clone()).collect();
    for set in generate_power_set(rest) {
        let mut new_set = vec![];
        new_set.push(head.clone());
        set.iter().for_each(|it| new_set.push(it.clone()));
        sets.push(new_set);
        sets.push(set);
    }
    sets
}
