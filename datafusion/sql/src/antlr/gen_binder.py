from dataclasses import dataclass
from enum import Enum, auto
import re
from typing import Dict, Generator, Iterable, List
from antlr4 import CommonTokenStream, InputStream
from antlr.ANTLRv4Lexer import ANTLRv4Lexer as Lexer
from antlr.ANTLRv4Parser import ANTLRv4Parser as Parser

BEGIN_COMMENT = "// Begin generated boiler plate"
END_COMMENT = "// End generated boiler plate"


@dataclass(frozen=True)
class MappingItem:
    logical_node: str
    grammar_node: str
    alt_ids: List[str]


def gen_mapping(grammar_file_name: str) -> Generator[MappingItem, None, None]:
    lexer = Lexer(InputStream(open(grammar_file_name).read()))
    stream = CommonTokenStream(lexer)
    parser = Parser(stream)
    grammar_spec = parser.grammarSpec()

    for rule_spec in grammar_spec.rules().ruleSpec():
        parser_rule_spec = rule_spec.parserRuleSpec()
        if parser_rule_spec is None:
            continue
        rule_ref = parser_rule_spec.RULE_REF()
        rule_prequels = parser_rule_spec.rulePrequel()
        if rule_prequels is None:
            continue
        for rule_prequel in rule_prequels:
            options_spec = rule_prequel.optionsSpec()

            if options_spec is None:
                continue
            options = options_spec.option()
            if options is None:
                continue
            for option in options:
                if option.identifier().getText() != "logical":
                    continue
                option_value = option.optionValue()
                m = re.match(r"'(.*)'", option_value.getText())
                if m is None:
                    continue
                rule_block = parser_rule_spec.ruleBlock()
                alt_ids = []
                for labeled_alt in rule_block.ruleAltList().labeledAlt():
                    alt_id = labeled_alt.identifier()
                    if alt_id is None:
                        continue
                    if alt_id.getText() in alt_ids:
                        continue
                    alt_ids.append(alt_id.getText())
                for node in m.group(1).split(","):
                    yield MappingItem(node, rule_ref.getText(), alt_ids)


class ImplEnum(Enum):
    ROUTE = auto()
    RULE = auto()
    ALT = auto()


@dataclass(frozen=True)
class Impl:
    type_: ImplEnum
    grammar_node: str
    logical_node: str
    code_block: str


def gen_old_code(text: str) -> Generator[Impl, None, None]:
    current_type = None
    current_grammar_node = None
    current_logical_node = None
    current_code_block = ""
    for line in text.split("\n"):
        if line.startswith("impl"):
            if current_grammar_node is not None:
                yield Impl(
                    current_type,
                    current_grammar_node,
                    current_logical_node,
                    current_code_block,
                )
                current_type = None
                current_grammar_node = None
                current_logical_node = None
            current_code_block = ""

            m = re.match(r"impl(?:<'.+>)? Binder<(.+?)(?:<'.+>)?> for (\w+)Context(All)?<'_> {", line)
            assert m is not None, f"bad line: {line}"
            has_all = m.group(3) == "All"
            current_type = current_type or (ImplEnum.RULE if has_all else ImplEnum.ALT)
            current_grammar_node = lower_first_letter(m.group(2))
            current_logical_node = m.group(1)
        elif line.startswith("#[route"):
            if current_grammar_node is not None:
                yield Impl(
                    current_type,
                    current_grammar_node,
                    current_logical_node,
                    current_code_block,
                )
                current_type = None
                current_grammar_node = None
                current_logical_node = None
                current_code_block = ""
            current_type = ImplEnum.ROUTE
        else:
            current_code_block += line + "\n"
    if current_grammar_node is not None:
        yield Impl(
            current_type, current_grammar_node, current_logical_node, current_code_block
        )


def gen_new_code(mapping: Iterable[MappingItem]) -> Generator[Impl, None, None]:
    for item in mapping:
        if len(item.alt_ids) > 0:
            yield Impl(ImplEnum.ROUTE, item.grammar_node, item.logical_node, "\n")
            for alt in item.alt_ids:
                yield Impl(ImplEnum.ALT, alt, item.logical_node, "\n")
        else:
            yield Impl(ImplEnum.RULE, item.grammar_node, item.logical_node, "\n")


def upper_first_letter(raw: str) -> str:
    return raw[0].upper() + raw[1:]


def lower_first_letter(raw: str) -> str:
    return raw[0].lower() + raw[1:]


def merge(
    old: Iterable[Impl], new: Iterable[Impl], alt_map: Dict[str, List[str]]
) -> Generator[str, None, None]:
    old_dict = {(impl.grammar_node, impl.logical_node): impl for impl in old}
    written = set()
    for impl in new:
        if impl.type_ == ImplEnum.ROUTE:
            yield "#[route("
            yield ",".join(alt_map[impl.grammar_node])
            yield ")]\n"
            yield "impl Binder<"
            yield impl.logical_node
            yield "> for "
            yield upper_first_letter(impl.grammar_node)
            yield "ContextAll<'_> {}\n"
        elif impl.type_ == ImplEnum.RULE or impl.type_ == ImplEnum.ALT:
            yield "impl"
            if impl.logical_node == "With":
                yield "<'a>"
            yield " Binder<"
            yield impl.logical_node
            if impl.logical_node == "With":
                yield "<'a>"
            yield "> for "
            yield upper_first_letter(impl.grammar_node)
            yield "Context"
            if impl.type_ == ImplEnum.RULE:
                yield "All"
            yield "<'_> {"
            if (impl.grammar_node, impl.logical_node) in old_dict and old_dict[impl.grammar_node, impl.logical_node].code_block.strip() != "":
                yield "\n"
                yield old_dict[impl.grammar_node, impl.logical_node].code_block
            else:
                yield "}\n"
        written.add((impl.grammar_node, impl.logical_node))
    for (grammar_node, logical_node), impl in sorted(
        old_dict.items(), key=lambda x: x[0]
    ):
        if (grammar_node, logical_node) not in written:
            if impl.type_ == ImplEnum.ROUTE:
                yield "#[route("
                yield ",".join(alt_map[impl.grammar_node])
                yield ")]\n"
                yield "impl Binder<"
                yield impl.logical_node
                yield "> for "
                yield upper_first_letter(impl.grammar_node)
                yield "ContextAll<'_> {}\n"
            elif impl.type_ == ImplEnum.RULE or impl.type_ == ImplEnum.ALT:
                yield "impl Binder<"
                yield impl.logical_node
                yield "> for "
                yield upper_first_letter(impl.grammar_node)
                yield "Context"
                if impl.type_ == ImplEnum.RULE:
                    yield "All"
                yield "<'_> {}\n"


def get_alt_map(mapping: Iterable[MappingItem]) -> Dict[str, List[str]]:
    result = {}
    for item in mapping:
        if len(item.alt_ids) > 0:
            result[item.grammar_node] = item.alt_ids
    return result


def main() -> None:
    grammar_file_name = "Presto.g4"
    binder_file_name = "../presto/planner.rs"

    mapping = list(gen_mapping(grammar_file_name))
    old_content = open(binder_file_name).read()
    start_index = old_content.find(BEGIN_COMMENT)
    end_index = old_content.find(END_COMMENT)

    alt_map = get_alt_map(mapping)

    merged_text = "".join(
        merge(
            gen_old_code(old_content[start_index + len(BEGIN_COMMENT) : end_index]),
            gen_new_code(mapping),
            alt_map,
        )
    )
    new_text = (
        old_content[: (start_index + len(BEGIN_COMMENT))]
        + "\n"
        + merged_text
        + old_content[end_index:]
    )
    with open(binder_file_name, "w") as f:
        f.write(new_text)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)
        import pdb
        import sys

        _, _, tb = sys.exc_info()
        pdb.post_mortem(tb)
