import html
from typing import Union

import mwparserfromhell
from wikitextparser import Table


def wiki_table_to_html(wiki_table: Union[Table, str]) -> str:

    if isinstance(wiki_table, Table):
        wiki_table_str = wiki_table.string
    elif isinstance(wiki_table, str):
        wiki_table_str = wiki_table
    else:
        raise TypeError("Only Table or string is accepted")

    # Fix unescaped HTML tags
    wiki_table_str = html.unescape(wiki_table_str)

    # Parse wikitable in string form using mwparserfromhell
    wiki_table_mw_parsed = mwparserfromhell.parse(wiki_table_str)
    wiki_table_tags = wiki_table_mw_parsed.filter_tags(matches="table")

    # Empty table
    if not wiki_table_tags:
        return ""

    mw_node = wiki_table_tags[0]

    result = ["<table>"]
    first_row = False
    exclamation_header_loop = False
    bar_header_loop = False

    for row in mw_node.contents.nodes:

        # The row is not a proper wiki_table node, so ignore
        if not hasattr(row, "wiki_markup"):
            continue

        # The header loop will not be necessary
        if row.wiki_markup == "|-":
            # Mark that the first row has been encountered
            # (it is just for header cases)
            first_row = True

        # Check if the header "!" loop is active and
        # if the current row is not a header cell
        if (
            exclamation_header_loop is True
            and isinstance(row, mwparserfromhell.nodes.Tag)
            and row.tag != "th"
        ):
            exclamation_header_loop = False
            result.append("</tr>")

        # Check if the header "|" loop is active and
        # if the current row is not a <td> or <th> cell
        if (
            bar_header_loop is True
            and isinstance(row, mwparserfromhell.nodes.Tag)
            and row.tag != "td"
        ):
            bar_header_loop = False
            result.append("</tr>")

        # Handle different cases:
        # Captions
        # "Exclamation" headers
        # "Bar" headers
        # "Normal" rows with "|-"

        # Handle optional case for captions
        if (
            isinstance(row, mwparserfromhell.nodes.Tag)
            and row.tag == "td"
            and row.contents.startswith("+")
            and row.wiki_markup == "|"
            and first_row is False
        ):
            # Extract the caption text, removing the '+' and
            # any leading/trailing whitespace
            caption_text = row.contents[1:].strip()
            result.append(f"<caption>{caption_text}</caption>")

        # Handle the optional case for header cells
        # that use exclamations and "\n"
        # ! Header 1
        # ! Header 2
        # ...
        elif (
            isinstance(row, mwparserfromhell.nodes.Tag)
            and row.tag == "th"
            and row.wiki_markup == "!"
            and first_row is False
        ):
            # If this is the first header cell,
            # start a new row and mark the header loop as active
            if not exclamation_header_loop:
                result.append("<tr>")
                exclamation_header_loop = True

            result.append("<th>")

            # Process the contents of the header cell
            for content in row.contents.nodes:
                if isinstance(content, mwparserfromhell.nodes.Text):
                    result.append(str(content))

            result.append("</th>")

        # Handle the special case for header cells
        # that use "|" and "\n"
        # | Header 1
        # | style = "text-align:center;" | Header 2
        # | style = "text-align:center;" | Header 3
        # ...
        elif (
            isinstance(row, mwparserfromhell.nodes.Tag)
            and row.tag == "td"
            and row.wiki_markup == "|"
            and first_row is False
        ):
            # If this is the first header cell,
            # start a new row and mark the header loop as active
            if not bar_header_loop:
                result.append("<tr>")
                bar_header_loop = True

            result.append("<th>")

            # Process the contents of the header cell
            for content in row.contents.nodes:
                if isinstance(content, mwparserfromhell.nodes.Text):
                    result.append(str(content))

            result.append("</th>")

        # Handle the default case for table rows
        if isinstance(row, mwparserfromhell.nodes.Tag) and row.tag == "tr":
            result.append("<tr>")

            for cell in row.contents.nodes:
                if isinstance(cell, mwparserfromhell.nodes.Tag) and cell.tag in [
                    "td",
                    "th",
                ]:
                    # Extract rowspan and colspan attributes if they exist
                    attrs = []
                    if any("rowspan" in attribute for attribute in cell.attributes):
                        for attribute in cell.attributes:
                            if "rowspan" in attribute:
                                attrs.append(attribute.strip())
                                break
                    if any("colspan" in attribute for attribute in cell.attributes):
                        for attribute in cell.attributes:
                            if "colspan" in attribute:
                                attrs.append(attribute.strip())
                                break

                    # Construct the opening cell tag with attributes (if any)
                    attrs_str = " ".join(attrs)
                    result.append(
                        f"<{cell.tag} {attrs_str}>" if attrs_str else f"<{cell.tag}>"
                    )

                    # Process the contents of the cell
                    for content in cell.contents.nodes:
                        if isinstance(content, mwparserfromhell.nodes.Text):
                            result.append(str(content))

                    # Close the cell tag
                    result.append(f"</{cell.tag}>")

            result.append("</tr>")  # Close the row

    # Close the table tag and return the result as a single string
    result.append("</table>")
    html_table = "".join(result)

    return html_table
