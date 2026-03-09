#!/usr/bin/env python3
"""Generate License.rtf from the plain-text LICENSE file for WiX installer."""

import os
import sys


def text_to_rtf(text: str) -> str:
    """Convert plain text to minimal RTF."""
    # Escape RTF special characters
    text = text.replace("\\", "\\\\")
    text = text.replace("{", "\\{")
    text = text.replace("}", "\\}")

    # Convert lines
    lines = text.split("\n")
    rtf_lines = []
    for line in lines:
        rtf_lines.append(line + "\\par")

    body = "\n".join(rtf_lines)

    return (
        "{\\rtf1\\ansi\\deff0"
        "{\\fonttbl{\\f0\\fswiss\\fcharset0 Calibri;}}"
        "\\viewkind4\\uc1\\pard\\f0\\fs20 "
        + body
        + "}"
    )


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    license_path = os.path.join(project_root, "LICENSE")
    output_path = os.path.join(project_root, "installer_build", "License.rtf")

    if not os.path.exists(license_path):
        print(f"Error: LICENSE file not found at {license_path}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(license_path, "r", encoding="utf-8") as f:
        license_text = f.read()

    rtf_content = text_to_rtf(license_text)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(rtf_content)

    print(f"Generated {output_path}")


if __name__ == "__main__":
    main()
