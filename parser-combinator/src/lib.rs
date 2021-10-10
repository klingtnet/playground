use std::ops::{Add, Deref};

use nom::branch::alt;
use nom::bytes::complete::{is_not, tag, take_till, take_until, take_while};
use nom::character::complete::{anychar, char, satisfy};
use nom::combinator::all_consuming;
use nom::multi::many1;
use nom::sequence::{delimited, preceded, separated_pair};
use nom::{AsChar, IResult};

#[derive(Debug, PartialEq, Eq, Clone, Default)]
struct Address {
    containers: Vec<String>,
    method: String,
}

fn address_part(input: &str) -> IResult<&str, &str> {
    preceded(char('/'), is_not(" \t\r\n#*,/?[]{}"))(input)
}

fn parse_address(path: &str) -> IResult<&str, Address> {
    // Parse at least one / preceded path part.
    let (input, parts) = all_consuming(many1(address_part))(path)?;

    let containers = parts
        .iter()
        .take(parts.len() - 1)
        .map(|p| p.deref().into())
        .collect();
    // Note that many1 ensures that we get at least one path part, hence it is safe to unwrap parts.last.
    let method = parts
        .last()
        .expect("BUG: no OSC address method")
        .deref()
        .into();
    Ok((input, Address { containers, method }))
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum AddressPattern {
    QuestionMark,
    Wildcard,
    BracketExpression(Vec<BracketExpression>),
    InvertedBracketExpression(Vec<BracketExpression>),
    Alt(String, String),
    Literal(String),
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum BracketExpression {
    Charset(Vec<char>),
    Range { from: char, to: char },
}

fn alnum_char(input: &str) -> IResult<&str, char> {
    satisfy(|b| b.is_alphanum())(input)
}

fn alnum_range(input: &str) -> IResult<&str, BracketExpression> {
    let (input, (from, to)) = separated_pair(alnum_char, char('-'), alnum_char)(input)?;
    Ok((input, BracketExpression::Range { from, to }))
}

fn charset(input: &str) -> IResult<&str, BracketExpression> {
    let (input, chars) = many1(anychar)(input)?;
    Ok((input, BracketExpression::Charset(chars)))
}

fn bracket(input: &str) -> IResult<&str, Vec<BracketExpression>> {
    many1(alt((alnum_range, charset)))(input)
}

fn inverted_bracket_expression(input: &str) -> IResult<&str, AddressPattern> {
    let (input, expr) = delimited(tag("[!"), take_until("]"), char(']'))(input)?;
    let (_, expr) = bracket(expr)?;
    Ok((input, AddressPattern::InvertedBracketExpression(expr)))
}

fn bracket_expression(input: &str) -> IResult<&str, AddressPattern> {
    let (input, expr) = delimited(char('['), take_until("]"), char(']'))(input)?;
    let (_, expr) = bracket(expr)?;
    Ok((input, AddressPattern::BracketExpression(expr)))
}

fn bracket_pattern(input: &str) -> IResult<&str, AddressPattern> {
    alt((inverted_bracket_expression, bracket_expression))(input)
}

fn alternative(input: &str) -> IResult<&str, AddressPattern> {
    let (input, (a, b)) = delimited(
        char('{'),
        separated_pair(take_until(","), char(','), take_until("}")),
        char('}'),
    )(input)?;
    Ok((input, AddressPattern::Alt(a.into(), b.into())))
}

fn questionmark(input: &str) -> IResult<&str, AddressPattern> {
    char('?')(input).map(|(input, _)| (input, AddressPattern::QuestionMark))
}

fn wildcard(input: &str) -> IResult<&str, AddressPattern> {
    char('*')(input).map(|(input, _)| (input, AddressPattern::Wildcard))
}

fn literal(input: &str) -> IResult<&str, AddressPattern> {
    Ok(("", AddressPattern::Literal(input.into())))
}

fn parse_pattern(input: &str) -> IResult<&str, AddressPattern> {
    alt((
        bracket_pattern,
        alternative,
        questionmark,
        wildcard,
        literal,
    ))(input)
}

fn parse_address_pattern(pattern: &str) -> IResult<&str, Vec<AddressPattern>> {
    let (input, pattern) = all_consuming(many1(preceded(char('/'), is_not(" \t\r\n/"))))(pattern)?;
    let patterns: Result<Vec<_>, _> = pattern
        .iter()
        .map(|part| parse_pattern(*part).map(|(_, pat)| pat))
        .collect();
    Ok((input, patterns.unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_address_pattern() {
        let (_, pat) =
            parse_address_pattern("/oscillator/[0-9]/*/[!1234]/{frequency,phase}/x?").unwrap();
        assert_eq!(
            pat,
            vec![
                AddressPattern::Literal("oscillator".into()),
                AddressPattern::BracketExpression(vec![BracketExpression::Range { from: '0', to: '9' }]),
                AddressPattern::Wildcard,
                AddressPattern::InvertedBracketExpression(vec![BracketExpression::Charset(vec![
                    '1', '2', '3', '4'
                ])]),
                AddressPattern::Alt("frequency".into(), "phase".into()),
                AddressPattern::Literal("x".into()),
                AddressPattern::QuestionMark
            ]
        );
    }

    #[test]
    fn test_parse_address() {
        assert_eq!(
            parse_address("/oscillator/4/voice/1/frequency"),
            Ok((
                "",
                Address {
                    containers: vec!["oscillator".into(), "4".into(), "voice".into(), "1".into()],
                    method: "frequency".into()
                }
            ))
        );
        assert_eq!(
            parse_address("/frequency"),
            Ok((
                "",
                Address {
                    containers: vec![],
                    method: "frequency".into()
                }
            ))
        );
    }

    #[test]
    fn test_invalid_addresses() {
        parse_address("").expect_err("empty address");
        parse_address("/").expect_err("bare root address");
        parse_address("//").expect_err("only slashes");
        parse_address("//container/method").expect_err("empty part");
        parse_address("/oscillators/[0-9]/frequency").expect_err("range pattern");
        parse_address("/oscillators/*/frequency").expect_err("wildcard pattern");
        parse_address("/oscillators/1?/frequency").expect_err("question mark pattern");
        parse_address("/oscillators/[abc]/{frequency,phase}").expect_err("choice pattern");
        parse_address("/oscillators/3/{frequency,phase}").expect_err("alternative pattern");
    }
}
