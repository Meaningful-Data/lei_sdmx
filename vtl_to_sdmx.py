"""
VTL to SDMX Structure Message Generator

This script converts VTL scripts in `input_metadata/` into SDMX structure
messages (TransformationScheme artefacts) that can be uploaded to an FMR
(Fusion Metadata Registry).

For each `.vtl` file it:
1. Parses the VTL script into a pysdmx `TransformationScheme` model object
   (via vtlengine's `generate_sdmx`).
2. Overrides the artefact identity (id / name) so it gets a stable, meaningful
   id in the registry instead of the auto-generated `TS1`.
3. Serializes it to an SDMX structure message under `output/`.

Output format note:
    We emit SDMX-JSON 2.0.0 by default. pysdmx's SDMX-ML writer applies a
    blanket single-quote -> double-quote replacement, which corrupts VTL
    expressions that escape reserved-word identifiers with single quotes
    (e.g. the `'errorlevel'` component produced by `check_datapoint`). FMR
    then rejects those as string literals ("expecting IDENTIFIER"). The
    SDMX-JSON writers preserve the expression text verbatim, so they round-trip
    correctly. FMR imports SDMX-JSON structure messages just like SDMX-ML.
    Set OUTPUT_FORMAT to Format.STRUCTURE_SDMX_ML_2_1 if you specifically need
    SDMX-ML and your scripts contain no single-quote-escaped identifiers.

Dependencies:
    - pysdmx
    - vtlengine
"""

from pathlib import Path

from msgspec import structs
from vtlengine import generate_sdmx

from pysdmx.io import write_sdmx
from pysdmx.io.format import Format


# Common identity for every generated TransformationScheme.
AGENCY_ID = "MD"
VERSION = "1.0"

# Output format and matching file extension. SDMX-JSON 2.0.0 preserves VTL
# expressions verbatim (see module docstring); FMR imports it natively.
OUTPUT_FORMAT = Format.STRUCTURE_SDMX_JSON_2_0_0
OUTPUT_EXTENSION = "json"

# Optional per-file overrides. Keys are the VTL file stem (filename without
# extension); values are the artefact id and name to use in the registry.
# Files not listed here default to: id = "LEI_<STEM>", name = "LEI <stem>".
ID_OVERRIDES = {
    "validation": {"id": "LEI_VALIDATIONS", "name": "LEI validations"},
    "calculation": {"id": "LEI_CALCULATION", "name": "LEI calculations"},
}


def _artefact_identity(vtl_path):
    """Return the (id, name) to assign to the TransformationScheme.

    Args:
        vtl_path (Path): Path to the VTL script.

    Returns:
        tuple[str, str]: The artefact id and human-readable name.
    """
    stem = vtl_path.stem
    if stem in ID_OVERRIDES:
        return ID_OVERRIDES[stem]["id"], ID_OVERRIDES[stem]["name"]
    return f"LEI_{stem.upper()}", f"LEI {stem}"


def convert_vtl_file(vtl_path, output_folder, agency_id=AGENCY_ID, version=VERSION):
    """Convert a single VTL script into an SDMX-ML 2.1 structure message.

    Args:
        vtl_path (Path): Path to the input `.vtl` script.
        output_folder (Path): Folder where the structure message is written.
        agency_id (str, optional): Agency ID for the artefact. Defaults to AGENCY_ID.
        version (str, optional): Version for the artefact. Defaults to VERSION.

    Returns:
        Path: Path to the written SDMX structure message file.
    """
    artefact_id, artefact_name = _artefact_identity(vtl_path)

    print(f"  Parsing '{vtl_path.name}' -> TransformationScheme '{artefact_id}'...")
    # vtlengine parses the VTL AST and builds a pysdmx TransformationScheme,
    # including any RulesetSchemes / UserDefinedOperatorSchemes it finds.
    ts = generate_sdmx(
        str(vtl_path), agency_id=agency_id, id=artefact_id, version=version
    )

    # generate_sdmx puts our id into the *name* but auto-generates the artefact
    # id ("TS1"). Override id/name so FMR gets a stable, meaningful identity.
    ts = structs.replace(ts, id=artefact_id, name=artefact_name)

    # RulesetSchemes and UserDefinedOperatorSchemes are *separate* maintainable
    # artefacts that the TransformationScheme only references. They also get
    # auto-generated ids ("RS1", "UDS1"); give them meaningful ids derived from
    # the parent and re-attach them to the scheme so the reference stays in sync.
    ruleset_schemes = [
        structs.replace(rs, id=f"{artefact_id}_RS{i}", name=f"{artefact_name} ruleset scheme {i}")
        for i, rs in enumerate(ts.ruleset_schemes, start=1)
    ]
    udo_schemes = [
        structs.replace(udo, id=f"{artefact_id}_UDS{i}", name=f"{artefact_name} UDO scheme {i}")
        for i, udo in enumerate(ts.user_defined_operator_schemes, start=1)
    ]
    ts = structs.replace(
        ts,
        ruleset_schemes=ruleset_schemes,
        user_defined_operator_schemes=udo_schemes,
    )

    print(
        f"    {len(ts.items)} transformation(s), "
        f"{len(ruleset_schemes)} ruleset scheme(s), "
        f"{len(udo_schemes)} UDO scheme(s) "
        f"[VTL {ts.vtl_version}]"
    )

    # Write the TransformationScheme together with every artefact it references,
    # so the message is self-contained and has no dangling references in FMR.
    artefacts = [ts, *ruleset_schemes, *udo_schemes]
    output_path = output_folder / f"{vtl_path.stem}.{OUTPUT_EXTENSION}"
    write_sdmx(
        artefacts,
        OUTPUT_FORMAT,
        output_path=str(output_path),
        prettyprint=True,
    )
    print(f"    Written: {output_path}")

    return output_path


def convert_folder(input_folder, output_folder):
    """Convert every `.vtl` file in `input_folder` to an SDMX structure message.

    Args:
        input_folder (Path): Folder containing `.vtl` scripts.
        output_folder (Path): Folder where the `.xml` messages are written.

    Returns:
        list[Path]: Paths to the written SDMX-ML files.
    """
    output_folder.mkdir(parents=True, exist_ok=True)

    vtl_files = sorted(input_folder.glob("*.vtl"))
    if not vtl_files:
        print(f"No .vtl files found in {input_folder}.")
        return []

    print(f"=== Converting {len(vtl_files)} VTL script(s) from {input_folder} ===")
    written = []
    for vtl_path in vtl_files:
        written.append(convert_vtl_file(vtl_path, output_folder))
    print(f"=== Done. {len(written)} SDMX structure message(s) written to {output_folder} ===")

    return written


if __name__ == "__main__":
    BASE_PATH = Path(__file__).parent
    INPUT_FOLDER = BASE_PATH / "input_metadata"
    OUTPUT_FOLDER = BASE_PATH / "output"

    convert_folder(INPUT_FOLDER, OUTPUT_FOLDER)
