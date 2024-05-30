
"""
HTML helper Utility. Should be called as html_utils.<function>
"""

def space():
    """
    Returns a space character
    """
    return "<p>&nbsp;</p>"


def link(text, url):
    """
    Returns a space character
    """
    return '<span><a href="{0}">{1}</a></span>'.format(url, text)


def body(msg, start=True, end=True):
    """
    Returns an html formatted body of the input text.
    Starts and ends the paragraph by default.
    """
    code = "{0}".format(msg.replace("{tab}", "&nbsp;" * 4))

    if start:
        code = (
            '<p><span style="font-family: calibri, sans-serif; font-size: 12pt;">'
            + code
        )

    if end:
        code += "</span></p>"

    return code


def list(contents, ordered=False):
    """
    Creates an HTML formatted ordered and unordered list. Expects a list and
    boolean for ordered/unordered.
    """

    if ordered:
        html_list = "ol"
    else:
        html_list = "ul"

    html = "<{0}>\n".format(html_list)
    for obj in contents:
        html += "    <li>{0}</li>\n".format(obj)
    html += "</{0}>".format(html_list)

    return html


def bullets(bullet_data):
    html = "<ul>\n"
    for proj in sorted(bullet_data):
        # Project
        html += "<li><strong>{0}</strong><ul>\n".format(proj)
        for error in bullet_data[proj]:
            # Entity
            line_item = "{timelog_id} | {user} / {task_link} - {entity} ".format(
                **error
            )
            html += '<li style="margin-left:10px;">{0}</li>\n'.format(line_item)
        html += "</ul>\n"
    html += "</ul>"

    return html


def signature():
    """
    Returns auto-reply email signature
    """
    return """
	<p><strong><span style="font-family: calibri, sans-serif; font-size: 12pt;">pam <span style="color: #3366ff;">/</span><span style="color: #ff0000;"><span style="color: #339966;">/</span>/</span> ai assistant <span style="color: #3366ff;">/</span><span style="color: #ff0000;"><span style="color: #339966;">/</span>/</span>&nbsp;alkemy&nbsp;x<br /></span></strong>
	<span style="font-family: calibri, sans-serif; font-size: 12pt;">nyc | phl | <span style="color: #f74a4a;">alkemy-x.com</span></span></p>
	"""


def stringify_list(list_obj, display="all", oxford=True, con="and"):
    if display.lower() == "first":
        list_obj = [i.split(" ")[0] for i in list_obj]

    con = con.strip().lower()

    if len(list_obj) == 2:
        conjunction = " {0} ".format(con)
    elif len(list_obj) > 2:
        if oxford:
            conjunction = ", {0} ".format(con)
        else:
            conjunction = " {0} ".format(con)
    else:
        conjunction = ""

    if len(list_obj) > 1:
        joined = ", ".join(list_obj[:-1]) + conjunction + list_obj[-1]
    elif len(list_obj) == 1:
        joined = list_obj[0]
    else:
        joined = " "

    return joined