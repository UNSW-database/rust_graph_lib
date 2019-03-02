//// WHERE a.name CONTAINS "a" AND (a.age + 5) > (10 * 3)
//// 0: ["a.name", "CONTAINS", "a", "AND", "(", "a.age", "+", "5", ")", ">", "(", "10", "*", "3", ")"]
//// 1: [PredicateExp{a.name, "a", CONTAINS}, "AND", "(", "a.age", "+", "5", ")", ">", ArithmeticExp(10, 3, *)]
//
//
//use property::filter::*;
//use json::JsonValue;
//
//pub fn parse(cypher: &Vec<&str>) -> Box<Expression>{
//    // add brackets
//    let mut pos: u32 = 0;
//    let mut result = JsonValue::as_bool(true);
//    while pos < cypher.len() {
//        if cypher[pos] == "(" {
//            let close: u32 = get_closing_bracket(cypher, pos);
//            let exp = parse(cypher[pos+1..close-1]);
//            result = convert_exp(result, exp, cypher[pos-1]);
//            pos = close + 2;
//        } else {
//            let exp = convert_val(cypher[pos]);
//            result = convert_exp(result, exp, cypher[pos-1]);
//            pos = pos + 2;
//        }
//    }
//}
//
//
//fn convert_val(exp: &str) -> Box<Expression> {
//    // get the Value Expression of a string
//}
//
//fn convert_exp(exp0: Box<Expression>, exp1: Box<Expression>, operator: &str) -> Box<Expression> {
//    // get the Predicate or Arithmetic Expression of a string
//}
//
//fn get_closing_bracket(cypher: &Vec<&str>, pos: u32) -> u32 {
//    // position of the closing bracket matching the current position
//}
