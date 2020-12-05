use std::collections::HashMap;

/**
* Will join all the elements of the first vector with all the elements of the second vector based a matching key
*/
pub fn inner_join<TLeft, TRight, TKey, TResult>(
    left: Vec<TLeft>, 
    right: Vec<TRight>, 
    left_key_selector: Box<dyn Fn(&TLeft) -> TKey>, 
    right_key_selector: Box<dyn Fn(&TRight) -> TKey>, 
    result_selector: Box<dyn Fn(TLeft, TRight) -> TResult>) -> Vec<TResult> 
    where TKey: std::cmp::Eq + std::hash::Hash + std::fmt::Debug, TRight: Clone + std::fmt::Debug, TLeft: Clone, TResult: std::fmt::Debug 
{
    let mut right_iter = right.iter();

    let mut results = Vec::new();
    let mut lookup: HashMap<TKey, Vec<TLeft>> = HashMap::new();
    for item in left
    {
        let key = left_key_selector(&item);
        let values = lookup.entry(key).or_insert(Vec::new());
        values.push(item);
    }
    while let Some(right_item) = right_iter.next() {
        let lefts: Option<&Vec<TLeft>> = lookup.get(&right_key_selector(&right_item));
        if let Some(lefts) = lefts {
            for left in lefts
            {
                results.push(result_selector(left.clone(), right_item.clone()));
            }
        }
    }

    results
}

/**
* Will join all elements from the first vector with the matching elements from the second vector based on a matching key
*/
pub fn left_join<TLeft, TRight, TKey, TResult>(
    left: Vec<TLeft>, 
    right: Vec<TRight>, 
    left_key_selector: Box<dyn Fn(&TLeft) -> TKey>, 
    right_key_selector: Box<dyn Fn(&TRight) -> TKey>, 
    result_selector: Box<dyn Fn(TLeft, Option<TRight>) -> TResult>) -> Vec<TResult> 
    where TKey: std::cmp::Eq + std::hash::Hash + std::fmt::Debug, TRight: Clone + std::fmt::Debug, TLeft: Clone, TResult: std::fmt::Debug 
{
    let mut left_iter = left.iter();

    let mut results = Vec::new();
    let mut lookup: HashMap<TKey, Vec<TRight>> = HashMap::new();
    for item in right
    {
        let key = right_key_selector(&item);
        let values = lookup.entry(key).or_insert(Vec::new());
        values.push(item);
    }
    while let Some(left_item) = left_iter.next() {
        let rights: Option<&Vec<TRight>> = lookup.get(&left_key_selector(&left_item));
        if let Some(rights) = rights {
            for right in rights
            {
                results.push(result_selector(left_item.clone(), Some(right.clone())));
            }
        } else {
            results.push(result_selector(left_item.clone(), None));
        }
    }

    results
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsString, path::PathBuf};

    use crate::core::file::{CoreFile, file_type::FileType};
    use super::*;

    const PATH_TO_TEST_DIR: &str = "./test/";

    #[test]
    fn test_inner_join() {
        let left_one = CoreFile { name: Some(OsString::from("left_one")), file_extension: None, file_type: Some(FileType::File), path: None };
        let left_two = CoreFile { name: Some(OsString::from("left_two")), file_extension: None, file_type: Some(FileType::File), path: None };
        let left = vec![left_one.clone(), left_two.clone()];

        let right_one = CoreFile { name: Some(OsString::from("right_one")), file_extension: None, file_type: Some(FileType::Dir), path: None };
        let right_two = CoreFile { name: Some(OsString::from("right_two")), file_extension: None, file_type: Some(FileType::File), path: None };
        let right = vec![right_one.clone(), right_two.clone()];

        let result = inner_join(left, right, Box::new(|l| l.file_type.clone()), Box::new(|r| r.file_type.clone()), Box::new(|l, r| vec![l, r]));

        assert_eq!(result, vec![vec![left_one, right_two.clone()], vec![left_two, right_two]]);
    }

    #[test]
    fn test_left_outer_join() {
        let left_one = CoreFile { name: Some(OsString::from("left_one")), file_extension: None, file_type: Some(FileType::File), path: None };
        let left_two = CoreFile { name: Some(OsString::from("left_two")), file_extension: None, file_type: Some(FileType::File), path: None };
        let left = vec![left_one.clone(), left_two.clone()];

        let right_one = CoreFile { name: Some(OsString::from("right_one")), file_extension: None, file_type: Some(FileType::Dir), path: None };
        let right_two = CoreFile { name: Some(OsString::from("right_two")), file_extension: None, file_type: Some(FileType::Dir), path: None };
        let right = vec![right_one.clone(), right_two.clone()];

        let result = left_join(left, right, Box::new(|l| l.file_type.clone()), Box::new(|r| r.file_type.clone()), Box::new(|l: CoreFile, r: Option<CoreFile>| vec![Some(l), r]));

        assert_eq!(result, vec![vec![Some(left_one), None], vec![Some(left_two), None]]);
    }
}