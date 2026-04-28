"""Converters between legacy parser models and unified Statute model.

These converters allow existing parsers to work with the new unified
architecture without requiring immediate refactoring.
"""

from datetime import datetime

from axiom.models_statute import Statute, StatuteSubsection


def _convert_subsections(subsections) -> list[StatuteSubsection]:
    """Convert legacy subsection objects to StatuteSubsection models.

    Handles both objects with a `children` attribute and those without.
    """
    return [
        StatuteSubsection(
            identifier=sub.identifier,
            heading=getattr(sub, "heading", None),
            text=sub.text,
            children=[
                StatuteSubsection(
                    identifier=c.identifier,
                    heading=getattr(c, "heading", None),
                    text=c.text,
                )
                for c in getattr(sub, "children", [])
            ],
        )
        for sub in subsections
    ]


def from_ca_section(ca_section) -> Statute:
    """Convert CA parser CASection to unified Statute.

    Args:
        ca_section: CASection from ca_statutes.py

    Returns:
        Unified Statute model
    """
    return Statute(
        jurisdiction="us-ca",
        code=ca_section.code,
        code_name=ca_section.code_name,
        section=ca_section.section_num,
        title=ca_section.title,
        text=ca_section.text,
        subsections=_convert_subsections(ca_section.subsections),
        division=ca_section.division,
        part=ca_section.part,
        chapter=ca_section.chapter,
        article=ca_section.article,
        history=ca_section.history,
        source_url=ca_section.url,
        retrieved_at=datetime.utcnow(),
    )


def from_fl_section(fl_section) -> Statute:
    """Convert FL parser FLSection to unified Statute.

    Args:
        fl_section: FLSection from fl_statutes.py

    Returns:
        Unified Statute model
    """
    return Statute(
        jurisdiction="us-fl",
        code=str(fl_section.chapter),
        code_name=fl_section.chapter_title,
        section=fl_section.number,
        title=fl_section.title,
        text=fl_section.text,
        chapter=str(fl_section.chapter),
        subsections=_convert_subsections(fl_section.subsections),
        source_url=fl_section.url,
        retrieved_at=datetime.utcnow(),
    )


def from_tx_section(tx_section) -> Statute:
    """Convert TX parser TXSection to unified Statute.

    Args:
        tx_section: TXSection from tx_statutes.py

    Returns:
        Unified Statute model
    """
    return Statute(
        jurisdiction="us-tx",
        code=tx_section.code,
        code_name=tx_section.code_name,
        section=tx_section.section,
        title=tx_section.title,
        text=tx_section.text,
        subsections=_convert_subsections(getattr(tx_section, "subsections", [])),
        source_url=tx_section.url,
        retrieved_at=datetime.utcnow(),
    )


def from_ny_section(ny_section) -> Statute:
    """Convert NY parser section to unified Statute.

    Args:
        ny_section: Section from ny_laws.py

    Returns:
        Unified Statute model
    """
    return Statute(
        jurisdiction="us-ny",
        code=ny_section.law_code,
        code_name=ny_section.law_name,
        section=ny_section.section,
        title=ny_section.title,
        text=ny_section.text,
        subsections=_convert_subsections(getattr(ny_section, "subsections", [])),
        source_url=ny_section.url,
        retrieved_at=datetime.utcnow(),
    )


def from_generic_state_section(state_section) -> Statute:
    """Convert generic state parser StateSection to unified Statute.

    Args:
        state_section: StateSection from generic_state.py

    Returns:
        Unified Statute model
    """
    # Map 2-letter state codes to jurisdiction IDs
    state_to_jurisdiction = {
        "OH": "us-oh",
        "PA": "us-pa",
        "IL": "us-il",
        "NC": "us-nc",
        "MI": "us-mi",
        "GA": "us-ga",
    }

    jurisdiction = state_to_jurisdiction.get(
        state_section.state, f"us-{state_section.state.lower()}"
    )

    return Statute(
        jurisdiction=jurisdiction,
        code=state_section.code,
        code_name=state_section.code_name,
        section=state_section.section_num,
        title=state_section.title,
        text=state_section.text,
        chapter=state_section.chapter,
        subsections=_convert_subsections(state_section.subsections),
        history=state_section.history,
        source_url=state_section.url,
        retrieved_at=datetime.utcnow(),
    )


def from_usc_section(section) -> Statute:
    """Convert US Code Section to unified Statute.

    Args:
        section: Section from models.py (existing USC model)

    Returns:
        Unified Statute model
    """
    return Statute(
        jurisdiction="us",
        code=str(section.citation.title),
        code_name=section.title_name,
        section=section.citation.section,
        subsection_path=section.citation.subsection,
        title=section.section_title,
        text=section.text,
        subsections=_convert_subsections(section.subsections),
        enacted_date=section.enacted_date,
        last_amended=section.last_amended,
        effective_date=section.effective_date,
        public_laws=section.public_laws,
        references_to=section.references_to,
        referenced_by=section.referenced_by,
        source_url=section.source_url,
        source_id=section.uslm_id,
        retrieved_at=datetime.combine(section.retrieved_at, datetime.min.time()),
    )
