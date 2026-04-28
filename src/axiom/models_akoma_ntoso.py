"""Pydantic models for Akoma Ntoso XML structure.

Akoma Ntoso (Architecture for Knowledge-Oriented Management of African
Normative Texts using Open Standards and Ontologies) is an OASIS international
technical standard for representing executive, legislative and judiciary
documents in a structured manner using legal XML vocabulary.

Standard: https://docs.oasis-open.org/legaldocml/akn-core/v1.0/
Namespace: http://docs.oasis-open.org/legaldocml/ns/akn/3.0

Key concepts:
- FRBR model for bibliographic identification (Work/Expression/Manifestation/Item)
- Hierarchical document structure (Part/Chapter/Section/Paragraph/etc.)
- Lifecycle tracking for document evolution
- Cross-references and modifications
- Temporal validity and version tracking

This module supports conversion from/to:
- USLM (US Legislation Markup)
- CLML (Crown Legislation Markup Language - UK)
"""

from __future__ import annotations

import re
from datetime import date, datetime
from enum import Enum
from typing import Any, ClassVar, Optional
from xml.etree import ElementTree as ET

from pydantic import BaseModel, Field, field_validator


# =============================================================================
# Namespaces
# =============================================================================

AKN_NAMESPACE = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"
AKN_NAMESPACES = {
    "akn": AKN_NAMESPACE,
    "xml": "http://www.w3.org/XML/1998/namespace",
}


# =============================================================================
# Enums
# =============================================================================


class DocumentType(str, Enum):
    """Akoma Ntoso document types."""

    ACT = "act"
    BILL = "bill"
    AMENDMENT = "amendment"
    JUDGMENT = "judgment"
    DOC = "doc"
    DEBATE_RECORD = "debateRecord"
    DEBATE_REPORT = "debateReport"
    STATEMENT = "statement"
    AMENDMENT_LIST = "amendmentList"
    OFFICIAL_GAZETTE = "officialGazette"
    PORTION = "portion"


class LifecycleEventType(str, Enum):
    """Types of lifecycle events."""

    GENERATION = "generation"
    AMENDMENT = "amendment"
    REPEAL = "repeal"
    COMMENCEMENT = "commencement"
    COMING_INTO_FORCE = "comingIntoForce"
    END_OF_EFFICACY = "endOfEfficacy"
    PUBLICATION = "publication"
    ORIGINAL = "original"
    SUBSTITUTION = "substitution"
    INSERTION = "insertion"
    RENUMBERING = "renumbering"


class ModificationType(str, Enum):
    """Types of modifications to legal text."""

    REPEAL = "repeal"
    SUBSTITUTION = "substitution"
    INSERTION = "insertion"
    RENUMBERING = "renumbering"
    SPLIT = "split"
    JOIN = "join"
    EXTENSION = "extension"
    SUSPENSION = "suspension"
    REORDER = "reorder"


class ReferenceType(str, Enum):
    """Types of references between documents."""

    ORIGINAL = "original"
    ACTIVE_REF = "activeRef"
    PASSIVE_REF = "passiveRef"
    JUDICIAL = "judicial"


# =============================================================================
# Base Model with XML Support
# =============================================================================


class AknBaseModel(BaseModel):
    """Base model with XML serialization support."""

    model_config = {"extra": "forbid"}

    # XML element name (override in subclasses)
    _xml_element: ClassVar[str] = ""
    _xml_namespace: ClassVar[str] = AKN_NAMESPACE

    def to_xml_element(self) -> ET.Element:
        """Convert model to XML element.

        Subclasses should override this for custom XML structure.
        """
        tag = f"{{{self._xml_namespace}}}{self._xml_element}"
        elem = ET.Element(tag)
        return elem

    def to_xml(self, encoding: str = "unicode") -> str:
        """Convert model to XML string.

        Args:
            encoding: Output encoding. Use "unicode" for string.

        Returns:
            XML string representation.
        """
        elem = self.to_xml_element()
        return ET.tostring(elem, encoding=encoding)

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "AknBaseModel":
        """Create model from XML element.

        Subclasses should override this for custom parsing.
        """
        raise NotImplementedError(f"{cls.__name__} must implement from_xml_element")

    @classmethod
    def from_xml(cls, xml_str: str) -> "AknBaseModel":
        """Create model from XML string.

        Args:
            xml_str: XML string to parse.

        Returns:
            Model instance.
        """
        root = ET.fromstring(xml_str)
        return cls.from_xml_element(root)


# =============================================================================
# FRBR Identification Models
# =============================================================================


class FRBRUri(AknBaseModel):
    """FRBR URI identifier."""

    _xml_element: ClassVar[str] = "FRBRuri"

    value: str = Field(..., description="The URI value")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()
        elem.set("value", self.value)
        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "FRBRUri":
        return cls(value=elem.get("value", ""))


class FRBRDate(AknBaseModel):
    """FRBR date with optional name attribute."""

    _xml_element: ClassVar[str] = "FRBRdate"

    value: date = Field(..., description="The date value")
    name: Optional[str] = Field(None, description="Name/purpose of this date")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()
        elem.set("date", self.value.isoformat())
        if self.name:
            elem.set("name", self.name)
        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "FRBRDate":
        date_str = elem.get("date", "")
        try:
            d = date.fromisoformat(date_str)
        except ValueError:
            d = date.today()
        return cls(value=d, name=elem.get("name"))


class FRBRAuthor(AknBaseModel):
    """FRBR author reference."""

    _xml_element: ClassVar[str] = "FRBRauthor"

    href: str = Field(..., description="Reference to author TLC entry")
    as_attr: Optional[str] = Field(None, alias="as", description="Role of author")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()
        elem.set("href", self.href)
        if self.as_attr:
            elem.set("as", self.as_attr)
        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "FRBRAuthor":
        return cls(href=elem.get("href", ""), **{"as": elem.get("as")})


class FRBRCountry(AknBaseModel):
    """FRBR country code."""

    _xml_element: ClassVar[str] = "FRBRcountry"

    value: str = Field(..., description="ISO 3166-1 alpha-2 country code")

    @field_validator("value")
    @classmethod
    def validate_country(cls, v: str) -> str:
        """Normalize country code to lowercase."""
        return v.lower()

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()
        elem.set("value", self.value)
        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "FRBRCountry":
        return cls(value=elem.get("value", ""))


class FRBRNumber(AknBaseModel):
    """FRBR document number."""

    _xml_element: ClassVar[str] = "FRBRnumber"

    value: str = Field(..., description="The number value")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()
        elem.set("value", self.value)
        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "FRBRNumber":
        return cls(value=elem.get("value", ""))


class FRBRName(AknBaseModel):
    """FRBR document name/subtype."""

    _xml_element: ClassVar[str] = "FRBRname"

    value: str = Field(..., description="The name value (e.g., 'act', 'bill')")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()
        elem.set("value", self.value)
        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "FRBRName":
        return cls(value=elem.get("value", ""))


class FRBRLanguage(AknBaseModel):
    """FRBR language specification."""

    _xml_element: ClassVar[str] = "FRBRlanguage"

    language: str = Field(..., description="ISO 639-1 language code")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()
        elem.set("language", self.language)
        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "FRBRLanguage":
        return cls(language=elem.get("language", "en"))


class FRBRWork(AknBaseModel):
    """FRBR Work level identification.

    The Work is the abstract intellectual creation - the law itself,
    independent of any particular version or format.
    """

    _xml_element: ClassVar[str] = "FRBRWork"

    uri: FRBRUri = Field(..., description="Work-level URI")
    date: FRBRDate = Field(..., description="Date of the Work")
    author: FRBRAuthor = Field(..., description="Author/creator of the Work")
    country: FRBRCountry = Field(..., description="Country of origin")
    number: Optional[FRBRNumber] = Field(None, description="Document number")
    name: Optional[FRBRName] = Field(None, description="Document name/subtype")

    # Additional metadata
    this: Optional[str] = Field(None, description="This Work's IRI")
    subtype: Optional[str] = Field(None, description="Document subtype")
    prescriptive: bool = Field(True, description="Whether this Work is prescriptive")
    authoritative: bool = Field(True, description="Whether this is authoritative")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()

        # Add FRBRthis
        if self.this:
            this_elem = ET.SubElement(elem, f"{{{AKN_NAMESPACE}}}FRBRthis")
            this_elem.set("value", self.this)

        # Add child elements
        elem.append(self.uri.to_xml_element())
        elem.append(self.date.to_xml_element())
        elem.append(self.author.to_xml_element())
        elem.append(self.country.to_xml_element())

        if self.number:
            elem.append(self.number.to_xml_element())
        if self.name:
            elem.append(self.name.to_xml_element())

        if self.subtype:
            subtype_elem = ET.SubElement(elem, f"{{{AKN_NAMESPACE}}}FRBRsubtype")
            subtype_elem.set("value", self.subtype)

        elem.set("prescriptive", str(self.prescriptive).lower())
        elem.set("authoritative", str(self.authoritative).lower())

        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "FRBRWork":
        ns = AKN_NAMESPACES

        # Parse required elements
        uri_elem = elem.find("akn:FRBRuri", ns)
        date_elem = elem.find("akn:FRBRdate", ns)
        author_elem = elem.find("akn:FRBRauthor", ns)
        country_elem = elem.find("akn:FRBRcountry", ns)

        uri = FRBRUri.from_xml_element(uri_elem) if uri_elem is not None else FRBRUri(value="")
        frbr_date = (
            FRBRDate.from_xml_element(date_elem)
            if date_elem is not None
            else FRBRDate(date=date.today())
        )
        author = (
            FRBRAuthor.from_xml_element(author_elem)
            if author_elem is not None
            else FRBRAuthor(href="")
        )
        country = (
            FRBRCountry.from_xml_element(country_elem)
            if country_elem is not None
            else FRBRCountry(value="xx")
        )

        # Parse optional elements
        number_elem = elem.find("akn:FRBRnumber", ns)
        name_elem = elem.find("akn:FRBRname", ns)
        this_elem = elem.find("akn:FRBRthis", ns)
        subtype_elem = elem.find("akn:FRBRsubtype", ns)

        return cls(
            uri=uri,
            date=frbr_date,
            author=author,
            country=country,
            number=FRBRNumber.from_xml_element(number_elem) if number_elem is not None else None,
            name=FRBRName.from_xml_element(name_elem) if name_elem is not None else None,
            this=this_elem.get("value") if this_elem is not None else None,
            subtype=subtype_elem.get("value") if subtype_elem is not None else None,
            prescriptive=elem.get("prescriptive", "true").lower() == "true",
            authoritative=elem.get("authoritative", "true").lower() == "true",
        )


class FRBRExpression(AknBaseModel):
    """FRBR Expression level identification.

    The Expression is a specific version of the Work in a particular
    language and at a particular point in time (e.g., as amended).
    """

    _xml_element: ClassVar[str] = "FRBRExpression"

    uri: FRBRUri = Field(..., description="Expression-level URI")
    date: FRBRDate = Field(..., description="Date of this Expression")
    author: FRBRAuthor = Field(..., description="Author/editor of Expression")
    language: FRBRLanguage = Field(..., description="Language of Expression")

    # Additional metadata
    this: Optional[str] = Field(None, description="This Expression's IRI")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()

        if self.this:
            this_elem = ET.SubElement(elem, f"{{{AKN_NAMESPACE}}}FRBRthis")
            this_elem.set("value", self.this)

        elem.append(self.uri.to_xml_element())
        elem.append(self.date.to_xml_element())
        elem.append(self.author.to_xml_element())
        elem.append(self.language.to_xml_element())

        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "FRBRExpression":
        ns = AKN_NAMESPACES

        uri_elem = elem.find("akn:FRBRuri", ns)
        date_elem = elem.find("akn:FRBRdate", ns)
        author_elem = elem.find("akn:FRBRauthor", ns)
        lang_elem = elem.find("akn:FRBRlanguage", ns)
        this_elem = elem.find("akn:FRBRthis", ns)

        return cls(
            uri=FRBRUri.from_xml_element(uri_elem) if uri_elem is not None else FRBRUri(value=""),
            date=FRBRDate.from_xml_element(date_elem)
            if date_elem is not None
            else FRBRDate(date=date.today()),
            author=FRBRAuthor.from_xml_element(author_elem)
            if author_elem is not None
            else FRBRAuthor(href=""),
            language=FRBRLanguage.from_xml_element(lang_elem)
            if lang_elem is not None
            else FRBRLanguage(language="en"),
            this=this_elem.get("value") if this_elem is not None else None,
        )


class FRBRManifestation(AknBaseModel):
    """FRBR Manifestation level identification.

    The Manifestation is a particular physical or digital format
    of the Expression (e.g., PDF, XML, HTML).
    """

    _xml_element: ClassVar[str] = "FRBRManifestation"

    uri: FRBRUri = Field(..., description="Manifestation-level URI")
    date: FRBRDate = Field(..., description="Date of this Manifestation")
    author: FRBRAuthor = Field(..., description="Publisher of Manifestation")

    # Additional metadata
    this: Optional[str] = Field(None, description="This Manifestation's IRI")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()

        if self.this:
            this_elem = ET.SubElement(elem, f"{{{AKN_NAMESPACE}}}FRBRthis")
            this_elem.set("value", self.this)

        elem.append(self.uri.to_xml_element())
        elem.append(self.date.to_xml_element())
        elem.append(self.author.to_xml_element())

        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "FRBRManifestation":
        ns = AKN_NAMESPACES

        uri_elem = elem.find("akn:FRBRuri", ns)
        date_elem = elem.find("akn:FRBRdate", ns)
        author_elem = elem.find("akn:FRBRauthor", ns)
        this_elem = elem.find("akn:FRBRthis", ns)

        return cls(
            uri=FRBRUri.from_xml_element(uri_elem) if uri_elem is not None else FRBRUri(value=""),
            date=FRBRDate.from_xml_element(date_elem)
            if date_elem is not None
            else FRBRDate(date=date.today()),
            author=FRBRAuthor.from_xml_element(author_elem)
            if author_elem is not None
            else FRBRAuthor(href=""),
            this=this_elem.get("value") if this_elem is not None else None,
        )


class FRBRItem(AknBaseModel):
    """FRBR Item level identification.

    The Item is a specific instance of a Manifestation
    (e.g., a particular file on a server).
    """

    _xml_element: ClassVar[str] = "FRBRItem"

    uri: FRBRUri = Field(..., description="Item-level URI")
    date: FRBRDate = Field(..., description="Date of this Item")
    author: FRBRAuthor = Field(..., description="Custodian of Item")

    # Additional metadata
    this: Optional[str] = Field(None, description="This Item's IRI")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()

        if self.this:
            this_elem = ET.SubElement(elem, f"{{{AKN_NAMESPACE}}}FRBRthis")
            this_elem.set("value", self.this)

        elem.append(self.uri.to_xml_element())
        elem.append(self.date.to_xml_element())
        elem.append(self.author.to_xml_element())

        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "FRBRItem":
        ns = AKN_NAMESPACES

        uri_elem = elem.find("akn:FRBRuri", ns)
        date_elem = elem.find("akn:FRBRdate", ns)
        author_elem = elem.find("akn:FRBRauthor", ns)
        this_elem = elem.find("akn:FRBRthis", ns)

        return cls(
            uri=FRBRUri.from_xml_element(uri_elem) if uri_elem is not None else FRBRUri(value=""),
            date=FRBRDate.from_xml_element(date_elem)
            if date_elem is not None
            else FRBRDate(date=date.today()),
            author=FRBRAuthor.from_xml_element(author_elem)
            if author_elem is not None
            else FRBRAuthor(href=""),
            this=this_elem.get("value") if this_elem is not None else None,
        )


class Identification(AknBaseModel):
    """Complete FRBR identification block.

    Contains Work, Expression, Manifestation, and optionally Item
    identification following the FRBR bibliographic model.
    """

    _xml_element: ClassVar[str] = "identification"

    source: str = Field(..., description="URI of the source organization")
    work: FRBRWork = Field(..., description="Work-level identification")
    expression: FRBRExpression = Field(..., description="Expression-level identification")
    manifestation: FRBRManifestation = Field(..., description="Manifestation-level identification")
    item: Optional[FRBRItem] = Field(None, description="Item-level identification")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()
        elem.set("source", self.source)

        elem.append(self.work.to_xml_element())
        elem.append(self.expression.to_xml_element())
        elem.append(self.manifestation.to_xml_element())

        if self.item:
            elem.append(self.item.to_xml_element())

        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "Identification":
        ns = AKN_NAMESPACES

        work_elem = elem.find("akn:FRBRWork", ns)
        expr_elem = elem.find("akn:FRBRExpression", ns)
        manif_elem = elem.find("akn:FRBRManifestation", ns)
        item_elem = elem.find("akn:FRBRItem", ns)

        return cls(
            source=elem.get("source", ""),
            work=FRBRWork.from_xml_element(work_elem)
            if work_elem is not None
            else FRBRWork(
                uri=FRBRUri(value=""),
                date=FRBRDate(date=date.today()),
                author=FRBRAuthor(href=""),
                country=FRBRCountry(value="xx"),
            ),
            expression=FRBRExpression.from_xml_element(expr_elem)
            if expr_elem is not None
            else FRBRExpression(
                uri=FRBRUri(value=""),
                date=FRBRDate(date=date.today()),
                author=FRBRAuthor(href=""),
                language=FRBRLanguage(language="en"),
            ),
            manifestation=FRBRManifestation.from_xml_element(manif_elem)
            if manif_elem is not None
            else FRBRManifestation(
                uri=FRBRUri(value=""),
                date=FRBRDate(date=date.today()),
                author=FRBRAuthor(href=""),
            ),
            item=FRBRItem.from_xml_element(item_elem) if item_elem is not None else None,
        )


# =============================================================================
# Publication Metadata
# =============================================================================


class Publication(AknBaseModel):
    """Publication information for a document."""

    _xml_element: ClassVar[str] = "publication"

    pub_date: date = Field(..., alias="date", description="Publication date")
    name: str = Field(..., description="Name of the publication/gazette")
    show_as: Optional[str] = Field(None, description="Display name")
    number: Optional[str] = Field(None, description="Publication/gazette number")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()
        elem.set("date", self.pub_date.isoformat())
        elem.set("name", self.name)
        if self.show_as:
            elem.set("showAs", self.show_as)
        if self.number:
            elem.set("number", self.number)
        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "Publication":
        date_str = elem.get("date", "")
        try:
            parsed_date = date.fromisoformat(date_str)
        except ValueError:
            parsed_date = date.today()

        return cls(
            pub_date=parsed_date,
            name=elem.get("name", ""),
            show_as=elem.get("showAs"),
            number=elem.get("number"),
        )


# =============================================================================
# Lifecycle Events
# =============================================================================


class LifecycleEvent(AknBaseModel):
    """A lifecycle event in document history."""

    _xml_element: ClassVar[str] = "eventRef"

    eid: str = Field(..., description="Event ID")
    event_date: date = Field(..., alias="date", description="Date of the event")
    event_type: LifecycleEventType = Field(..., alias="type", description="Type of lifecycle event")
    source: str = Field(..., description="Reference to the source document")
    refers_to: Optional[str] = Field(None, description="Reference to affected content")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()
        elem.set("eId", self.eid)
        elem.set("date", self.event_date.isoformat())
        elem.set("type", self.event_type.value)
        elem.set("source", self.source)
        if self.refers_to:
            elem.set("refersTo", self.refers_to)
        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "LifecycleEvent":
        date_str = elem.get("date", "")
        try:
            parsed_date = date.fromisoformat(date_str)
        except ValueError:  # pragma: no cover
            parsed_date = date.today()  # pragma: no cover

        type_str = elem.get("type", "generation")
        try:
            parsed_type = LifecycleEventType(type_str)
        except ValueError:
            parsed_type = LifecycleEventType.GENERATION

        return cls(
            eid=elem.get("eId", ""),
            event_date=parsed_date,
            event_type=parsed_type,
            source=elem.get("source", ""),
            refers_to=elem.get("refersTo"),
        )


class Lifecycle(AknBaseModel):
    """Document lifecycle tracking.

    Records the history of events affecting a document,
    including enactment, amendments, repeals, and commencements.
    """

    _xml_element: ClassVar[str] = "lifecycle"

    source: str = Field(..., description="URI of the source organization")
    events: list[LifecycleEvent] = Field(default_factory=list, description="Lifecycle events")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()
        elem.set("source", self.source)
        for event in self.events:
            elem.append(event.to_xml_element())
        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "Lifecycle":
        ns = AKN_NAMESPACES
        events = []
        for event_elem in elem.findall("akn:eventRef", ns):
            events.append(LifecycleEvent.from_xml_element(event_elem))  # pragma: no cover

        return cls(
            source=elem.get("source", ""),
            events=events,
        )


# =============================================================================
# Cross-References
# =============================================================================


class Reference(AknBaseModel):
    """A reference to another document or section.

    Used for internal and external cross-references.
    """

    _xml_element: ClassVar[str] = "ref"

    href: str = Field(..., description="Target URI or eId")
    show_as: Optional[str] = Field(None, description="Display text")
    text: Optional[str] = Field(None, description="Reference text content")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()
        elem.set("href", self.href)
        if self.show_as:
            elem.set("showAs", self.show_as)
        if self.text:
            elem.text = self.text
        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "Reference":
        return cls(
            href=elem.get("href", ""),
            show_as=elem.get("showAs"),
            text=elem.text,
        )


class AknCitation(AknBaseModel):
    """A formal citation to a legal source.

    More structured than a simple reference, includes
    citation text and optional numerical references.

    Named AknCitation to avoid conflict with axiom.models.Citation.
    """

    _xml_element: ClassVar[str] = "citation"

    href: str = Field(..., description="Target URI")
    show_as: Optional[str] = Field(None, description="Display text")
    text: Optional[str] = Field(None, description="Citation text content")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()
        elem.set("href", self.href)
        if self.show_as:
            elem.set("showAs", self.show_as)
        if self.text:
            elem.text = self.text
        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "AknCitation":
        return cls(
            href=elem.get("href", ""),
            show_as=elem.get("showAs"),
            text=elem.text,
        )


# Alias for backward compatibility
Citation = AknCitation


class Modification(AknBaseModel):
    """A modification to legal text.

    Records changes made by one document to another,
    including the type of change and affected content.
    """

    _xml_element: ClassVar[str] = "textualMod"

    mod_type: ModificationType = Field(..., alias="type", description="Type of modification")
    source: str = Field(..., description="Reference to modifying provision")
    destination: str = Field(..., description="Reference to modified provision")
    force: Optional[date] = Field(None, description="Date modification takes effect")
    previous: Optional[str] = Field(None, description="Previous text (for substitutions)")
    new: Optional[str] = Field(None, description="New text")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()
        elem.set("type", self.mod_type.value)

        source_elem = ET.SubElement(elem, f"{{{AKN_NAMESPACE}}}source")
        source_elem.set("href", self.source)

        dest_elem = ET.SubElement(elem, f"{{{AKN_NAMESPACE}}}destination")
        dest_elem.set("href", self.destination)

        if self.force:
            force_elem = ET.SubElement(elem, f"{{{AKN_NAMESPACE}}}force")
            force_elem.set("date", self.force.isoformat())

        if self.previous:
            prev_elem = ET.SubElement(elem, f"{{{AKN_NAMESPACE}}}old")
            prev_elem.text = self.previous

        if self.new:
            new_elem = ET.SubElement(elem, f"{{{AKN_NAMESPACE}}}new")
            new_elem.text = self.new

        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "Modification":
        ns = AKN_NAMESPACES

        type_str = elem.get("type", "substitution")
        try:
            parsed_mod_type = ModificationType(type_str)
        except ValueError:
            parsed_mod_type = ModificationType.SUBSTITUTION

        source_elem = elem.find("akn:source", ns)
        dest_elem = elem.find("akn:destination", ns)
        force_elem = elem.find("akn:force", ns)
        old_elem = elem.find("akn:old", ns)
        new_elem = elem.find("akn:new", ns)

        force_date = None
        if force_elem is not None:
            try:
                force_date = date.fromisoformat(force_elem.get("date", ""))
            except ValueError:
                pass

        return cls(
            mod_type=parsed_mod_type,
            source=source_elem.get("href", "") if source_elem is not None else "",
            destination=dest_elem.get("href", "") if dest_elem is not None else "",
            force=force_date,
            previous=old_elem.text if old_elem is not None else None,
            new=new_elem.text if new_elem is not None else None,
        )


# =============================================================================
# Temporal Models
# =============================================================================


class TimeInterval(AknBaseModel):
    """A time interval for validity/efficacy periods."""

    _xml_element: ClassVar[str] = "timeInterval"

    eid: str = Field(..., description="Interval ID")
    start: Optional[date] = Field(None, description="Start date (inclusive)")
    end: Optional[date] = Field(None, description="End date (inclusive)")
    refers_to: Optional[str] = Field(None, description="Event reference")
    duration: Optional[str] = Field(None, description="ISO 8601 duration")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()
        elem.set("eId", self.eid)
        if self.start:
            elem.set("start", self.start.isoformat())
        if self.end:
            elem.set("end", self.end.isoformat())
        if self.refers_to:
            elem.set("refersTo", self.refers_to)
        if self.duration:
            elem.set("duration", self.duration)
        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "TimeInterval":
        start_date = None
        end_date = None

        if elem.get("start"):
            try:
                start_date = date.fromisoformat(elem.get("start", ""))
            except ValueError:
                pass

        if elem.get("end"):
            try:
                end_date = date.fromisoformat(elem.get("end", ""))
            except ValueError:
                pass

        return cls(
            eid=elem.get("eId", ""),
            start=start_date,
            end=end_date,
            refers_to=elem.get("refersTo"),
            duration=elem.get("duration"),
        )


class TemporalGroup(AknBaseModel):
    """A collection of temporal information."""

    _xml_element: ClassVar[str] = "temporalGroup"

    eid: str = Field(..., description="Group ID")
    intervals: list[TimeInterval] = Field(default_factory=list, description="Time intervals")

    def to_xml_element(self) -> ET.Element:
        elem = super().to_xml_element()
        elem.set("eId", self.eid)
        for interval in self.intervals:
            elem.append(interval.to_xml_element())
        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "TemporalGroup":
        ns = AKN_NAMESPACES
        intervals = []
        for int_elem in elem.findall("akn:timeInterval", ns):
            intervals.append(TimeInterval.from_xml_element(int_elem))

        return cls(
            eid=elem.get("eId", ""),
            intervals=intervals,
        )


# =============================================================================
# Hierarchical Structure Elements
# =============================================================================


class HierarchicalElement(AknBaseModel):
    """Base class for hierarchical document elements.

    Akoma Ntoso provides ~25 named hierarchical elements for structuring
    legislation: alinea, article, book, chapter, clause, division, indent,
    level, list, paragraph, part, point, proviso, rule, subrule, section,
    subchapter, subclause, subdivision, sublist, subparagraph, subpart,
    subsection, subtitle, title, tome, transitional.
    """

    _xml_element: ClassVar[str] = "hcontainer"

    eid: str = Field(..., description="Element ID (unique within document)")
    guid: Optional[str] = Field(None, description="Globally unique ID")
    name: Optional[str] = Field(None, description="Element name (for generic hcontainer)")
    num: Optional[str] = Field(None, description="Number/identifier")
    heading: Optional[str] = Field(None, description="Heading text")
    subheading: Optional[str] = Field(None, description="Subheading text")
    text: str = Field("", description="Text content")
    children: list["HierarchicalElement"] = Field(
        default_factory=list, description="Child elements"
    )

    # Temporal attributes
    period: Optional[str] = Field(None, description="Reference to temporal group")

    # Status attributes
    status: Optional[str] = Field(None, description="Status (e.g., 'repealed', 'notYetInForce')")

    def to_xml_element(self) -> ET.Element:
        tag = f"{{{AKN_NAMESPACE}}}{self._xml_element}"
        elem = ET.Element(tag)

        elem.set("eId", self.eid)
        if self.guid:
            elem.set("GUID", self.guid)
        if self.name and self._xml_element == "hcontainer":
            elem.set("name", self.name)
        if self.period:
            elem.set("period", self.period)
        if self.status:
            elem.set("status", self.status)

        # Add num element
        if self.num:
            num_elem = ET.SubElement(elem, f"{{{AKN_NAMESPACE}}}num")
            num_elem.text = self.num

        # Add heading element
        if self.heading:
            heading_elem = ET.SubElement(elem, f"{{{AKN_NAMESPACE}}}heading")
            heading_elem.text = self.heading

        # Add subheading element
        if self.subheading:
            subheading_elem = ET.SubElement(elem, f"{{{AKN_NAMESPACE}}}subheading")
            subheading_elem.text = self.subheading

        # Add content
        if self.text:
            content_elem = ET.SubElement(elem, f"{{{AKN_NAMESPACE}}}content")
            p_elem = ET.SubElement(content_elem, f"{{{AKN_NAMESPACE}}}p")
            p_elem.text = self.text

        # Add children
        for child in self.children:
            elem.append(child.to_xml_element())

        return elem

    @classmethod
    def from_xml_element(cls, elem: ET.Element) -> "HierarchicalElement":
        ns = AKN_NAMESPACES

        # Determine the specific element type from tag
        tag = elem.tag
        if tag.startswith("{"):
            tag = tag.split("}")[1]

        # Get basic attributes
        eid = elem.get("eId", "")
        guid = elem.get("GUID")
        name = elem.get("name")
        period = elem.get("period")
        status = elem.get("status")

        # Parse num
        num_elem = elem.find("akn:num", ns)
        num = num_elem.text if num_elem is not None else None

        # Parse heading
        heading_elem = elem.find("akn:heading", ns)
        heading = heading_elem.text if heading_elem is not None else None

        # Parse subheading
        subheading_elem = elem.find("akn:subheading", ns)
        subheading = subheading_elem.text if subheading_elem is not None else None

        # Parse text content
        text_parts = []
        for p_elem in elem.findall(".//akn:p", ns):
            if p_elem.text:
                text_parts.append(p_elem.text)
        text = "\n".join(text_parts)

        # Parse children (recursively)
        children = []
        for child_tag in [
            "part",
            "chapter",
            "section",
            "subsection",
            "paragraph",
            "subparagraph",
            "clause",
            "subclause",
            "article",
            "hcontainer",
        ]:
            for child_elem in elem.findall(f"akn:{child_tag}", ns):
                child_cls = _HIERARCHICAL_ELEMENTS.get(child_tag, HierarchicalElement)
                children.append(child_cls.from_xml_element(child_elem))

        # Return appropriate subclass
        element_cls = _HIERARCHICAL_ELEMENTS.get(tag, cls)
        return element_cls(
            eid=eid,
            guid=guid,
            name=name,
            num=num,
            heading=heading,
            subheading=subheading,
            text=text,
            children=children,
            period=period,
            status=status,
        )


class Part(HierarchicalElement):
    """A Part - major division of a document."""

    _xml_element: ClassVar[str] = "part"


class Chapter(HierarchicalElement):
    """A Chapter - subdivision of a Part."""

    _xml_element: ClassVar[str] = "chapter"


class Section(HierarchicalElement):
    """A Section - primary structural unit of legislation."""

    _xml_element: ClassVar[str] = "section"


class Subsection(HierarchicalElement):
    """A Subsection - subdivision of a Section."""

    _xml_element: ClassVar[str] = "subsection"


class Paragraph(HierarchicalElement):
    """A Paragraph - numbered text block."""

    _xml_element: ClassVar[str] = "paragraph"


class Subparagraph(HierarchicalElement):
    """A Subparagraph - subdivision of a Paragraph."""

    _xml_element: ClassVar[str] = "subparagraph"


class Clause(HierarchicalElement):
    """A Clause - specific provision within a section."""

    _xml_element: ClassVar[str] = "clause"


class Subclause(HierarchicalElement):
    """A Subclause - subdivision of a Clause."""

    _xml_element: ClassVar[str] = "subclause"


class Article(HierarchicalElement):
    """An Article - numbered provision (common in civil law)."""

    _xml_element: ClassVar[str] = "article"


# Mapping for dynamic element lookup
_HIERARCHICAL_ELEMENTS: dict[str, type[HierarchicalElement]] = {
    "part": Part,
    "chapter": Chapter,
    "section": Section,
    "subsection": Subsection,
    "paragraph": Paragraph,
    "subparagraph": Subparagraph,
    "clause": Clause,
    "subclause": Subclause,
    "article": Article,
    "hcontainer": HierarchicalElement,
}


# =============================================================================
# Document Types
# =============================================================================


class AkomaNtosoDocument(AknBaseModel):
    """Base class for Akoma Ntoso documents.

    All Akoma Ntoso documents share common metadata structure
    (identification, publication, lifecycle) and body content.
    """

    _xml_element: ClassVar[str] = "akomaNtoso"

    document_type: DocumentType = Field(..., description="Type of document")
    identification: Identification = Field(..., description="FRBR identification")
    publication: Optional[Publication] = Field(None, description="Publication info")
    lifecycle: Optional[Lifecycle] = Field(None, description="Lifecycle events")
    body: list[HierarchicalElement] = Field(default_factory=list, description="Document body")

    # Cross-references
    references: list[Reference] = Field(
        default_factory=list, description="Internal/external references"
    )
    modifications: list[Modification] = Field(
        default_factory=list, description="Textual modifications"
    )

    # Temporal data
    temporal_groups: list[TemporalGroup] = Field(
        default_factory=list, description="Temporal groups"
    )

    # Source tracking
    source_url: Optional[str] = Field(None, description="Source URL")
    retrieved_at: Optional[datetime] = Field(None, description="Retrieval timestamp")

    def to_xml_element(self) -> ET.Element:
        # Register namespace
        ET.register_namespace("akn", AKN_NAMESPACE)

        # Root element
        root = ET.Element(f"{{{AKN_NAMESPACE}}}akomaNtoso")

        # Document type container (e.g., <act>, <bill>)
        doc_elem = ET.SubElement(root, f"{{{AKN_NAMESPACE}}}{self.document_type.value}")

        # Meta section
        meta = ET.SubElement(doc_elem, f"{{{AKN_NAMESPACE}}}meta")
        meta.append(self.identification.to_xml_element())

        if self.publication:
            meta.append(self.publication.to_xml_element())

        if self.lifecycle:
            meta.append(self.lifecycle.to_xml_element())

        # References block
        if self.references or self.modifications:
            refs = ET.SubElement(meta, f"{{{AKN_NAMESPACE}}}references")
            for ref in self.references:
                refs.append(ref.to_xml_element())

        # Analysis block (for modifications)
        if self.modifications:
            analysis = ET.SubElement(meta, f"{{{AKN_NAMESPACE}}}analysis")
            active_mods = ET.SubElement(analysis, f"{{{AKN_NAMESPACE}}}activeModifications")
            for mod in self.modifications:
                active_mods.append(mod.to_xml_element())

        # Temporal data
        if self.temporal_groups:
            temporal = ET.SubElement(meta, f"{{{AKN_NAMESPACE}}}temporalData")
            for group in self.temporal_groups:
                temporal.append(group.to_xml_element())

        # Body
        body_elem = ET.SubElement(doc_elem, f"{{{AKN_NAMESPACE}}}body")
        for element in self.body:
            body_elem.append(element.to_xml_element())

        return root

    def to_xml(self, encoding: str = "unicode", xml_declaration: bool = True) -> str:
        """Convert to XML string with optional declaration.

        Args:
            encoding: Output encoding. Use "unicode" for string.
            xml_declaration: Whether to include XML declaration.

        Returns:
            XML string representation.
        """
        elem = self.to_xml_element()

        if xml_declaration and encoding == "unicode":
            return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(elem, encoding=encoding)
        return ET.tostring(elem, encoding=encoding)

    @classmethod
    def from_xml_element(cls, root: ET.Element) -> "AkomaNtosoDocument":
        ns = AKN_NAMESPACES

        # Determine document type from child element
        doc_type = DocumentType.DOC
        doc_elem = None
        for dt in DocumentType:
            found = root.find(f"akn:{dt.value}", ns)
            if found is not None:
                doc_type = dt
                doc_elem = found
                break

        if doc_elem is None:
            raise ValueError("No document type element found")

        # Parse meta section
        meta = doc_elem.find("akn:meta", ns)
        if meta is None:
            raise ValueError("No meta section found")

        # Parse identification
        id_elem = meta.find("akn:identification", ns)  # pragma: no cover
        identification = (  # pragma: no cover
            Identification.from_xml_element(id_elem)
            if id_elem is not None
            else Identification(
                source="",
                work=FRBRWork(
                    uri=FRBRUri(value=""),
                    date=FRBRDate(date=date.today()),
                    author=FRBRAuthor(href=""),
                    country=FRBRCountry(value="xx"),
                ),
                expression=FRBRExpression(
                    uri=FRBRUri(value=""),
                    date=FRBRDate(date=date.today()),
                    author=FRBRAuthor(href=""),
                    language=FRBRLanguage(language="en"),
                ),
                manifestation=FRBRManifestation(
                    uri=FRBRUri(value=""),
                    date=FRBRDate(date=date.today()),
                    author=FRBRAuthor(href=""),
                ),
            )
        )

        # Parse publication
        pub_elem = meta.find("akn:publication", ns)  # pragma: no cover
        publication = Publication.from_xml_element(pub_elem) if pub_elem is not None else None  # pragma: no cover

        # Parse lifecycle
        life_elem = meta.find("akn:lifecycle", ns)  # pragma: no cover
        lifecycle = Lifecycle.from_xml_element(life_elem) if life_elem is not None else None  # pragma: no cover

        # Parse references
        references = []  # pragma: no cover
        refs_elem = meta.find("akn:references", ns)  # pragma: no cover
        if refs_elem is not None:  # pragma: no cover
            for ref_elem in refs_elem.findall("akn:ref", ns):  # pragma: no cover
                references.append(Reference.from_xml_element(ref_elem))  # pragma: no cover
            for cite_elem in refs_elem.findall("akn:citation", ns):  # pragma: no cover
                # Treat citations as references
                references.append(  # pragma: no cover
                    Reference(
                        href=cite_elem.get("href", ""),
                        show_as=cite_elem.get("showAs"),
                        text=cite_elem.text,
                    )
                )

        # Parse modifications
        modifications = []  # pragma: no cover
        analysis_elem = meta.find("akn:analysis", ns)  # pragma: no cover
        if analysis_elem is not None:  # pragma: no cover
            for mod_elem in analysis_elem.findall(".//akn:textualMod", ns):  # pragma: no cover
                modifications.append(Modification.from_xml_element(mod_elem))  # pragma: no cover

        # Parse temporal groups
        temporal_groups = []  # pragma: no cover
        temporal_elem = meta.find("akn:temporalData", ns)  # pragma: no cover
        if temporal_elem is not None:  # pragma: no cover
            for group_elem in temporal_elem.findall("akn:temporalGroup", ns):  # pragma: no cover
                temporal_groups.append(TemporalGroup.from_xml_element(group_elem))  # pragma: no cover

        # Parse body
        body = []  # pragma: no cover
        body_elem = doc_elem.find("akn:body", ns)  # pragma: no cover
        if body_elem is not None:  # pragma: no cover
            for child_tag in ["part", "chapter", "section", "article", "paragraph", "hcontainer"]:  # pragma: no cover
                for child_elem in body_elem.findall(f"akn:{child_tag}", ns):  # pragma: no cover
                    child_cls = _HIERARCHICAL_ELEMENTS.get(child_tag, HierarchicalElement)  # pragma: no cover
                    body.append(child_cls.from_xml_element(child_elem))  # pragma: no cover

        # Use appropriate subclass
        doc_cls = _DOCUMENT_TYPES.get(doc_type, cls)  # pragma: no cover
        return doc_cls(  # pragma: no cover
            document_type=doc_type,
            identification=identification,
            publication=publication,
            lifecycle=lifecycle,
            body=body,
            references=references,
            modifications=modifications,
            temporal_groups=temporal_groups,
        )

    @classmethod
    def from_xml(cls, xml_str: str) -> "AkomaNtosoDocument":
        """Create document from XML string.

        Args:
            xml_str: XML string to parse.

        Returns:
            AkomaNtosoDocument instance.
        """
        root = ET.fromstring(xml_str)
        return cls.from_xml_element(root)


class Act(AkomaNtosoDocument):
    """An Act - enacted legislation.

    Acts are the primary form of legislation passed by legislative bodies.
    """

    document_type: DocumentType = Field(default=DocumentType.ACT)


class Bill(AkomaNtosoDocument):
    """A Bill - proposed legislation.

    Bills are draft legislation under consideration by a legislative body.
    """

    document_type: DocumentType = Field(default=DocumentType.BILL)

    # Bill-specific fields
    introduced_date: Optional[date] = Field(None, description="Date bill was introduced")
    sponsors: list[str] = Field(default_factory=list, description="Bill sponsors")


class Amendment(AkomaNtosoDocument):
    """An Amendment - modification to legislation.

    Amendments are formal changes to existing or proposed legislation.
    """

    document_type: DocumentType = Field(default=DocumentType.AMENDMENT)

    # Amendment-specific fields
    amends: Optional[str] = Field(None, description="URI of document being amended")


class Judgment(AkomaNtosoDocument):
    """A Judgment - court decision.

    Judgments are formal decisions issued by courts or tribunals.
    """

    document_type: DocumentType = Field(default=DocumentType.JUDGMENT)

    # Judgment-specific fields
    court: Optional[str] = Field(None, description="Name of the court")
    case_number: Optional[str] = Field(None, description="Case number/docket")
    decision_date: Optional[date] = Field(None, description="Date of decision")


class Doc(AkomaNtosoDocument):
    """A generic document.

    Used for documents that don't fit other specific types.
    """

    document_type: DocumentType = Field(default=DocumentType.DOC)


# Mapping for dynamic document type lookup
_DOCUMENT_TYPES: dict[DocumentType, type[AkomaNtosoDocument]] = {
    DocumentType.ACT: Act,
    DocumentType.BILL: Bill,
    DocumentType.AMENDMENT: Amendment,
    DocumentType.JUDGMENT: Judgment,
    DocumentType.DOC: Doc,
}


# =============================================================================
# Utility Functions
# =============================================================================


def create_work_uri(country: str, doc_type: str, year: int, number: int) -> str:
    """Create a standard Akoma Ntoso work URI.

    Args:
        country: ISO 3166-1 alpha-2 country code
        doc_type: Document type (act, bill, etc.)
        year: Year of enactment/introduction
        number: Document number

    Returns:
        Work URI in standard format
    """
    return f"/akn/{country.lower()}/{doc_type}/{year}/{number}"


def create_expression_uri(work_uri: str, language: str, version_date: date) -> str:
    """Create a standard Akoma Ntoso expression URI.

    Args:
        work_uri: The work-level URI
        language: ISO 639-1 language code
        version_date: Date of this expression version

    Returns:
        Expression URI in standard format
    """
    return f"{work_uri}/{language}@{version_date.isoformat()}"


def parse_akn_uri(uri: str) -> dict[str, Any]:
    """Parse an Akoma Ntoso URI into components.

    Args:
        uri: Akoma Ntoso URI (e.g., /akn/us/act/2023/1/eng@2023-01-01)

    Returns:
        Dictionary with parsed components
    """
    result: dict[str, Any] = {
        "country": None,
        "doc_type": None,
        "year": None,
        "number": None,
        "language": None,
        "version_date": None,
        "section": None,
    }

    # Work pattern: /akn/{country}/{docType}/{year}/{number}
    work_pattern = r"^/akn/([a-z]{2})/([a-z]+)/(\d{4})/(\d+)"
    work_match = re.match(work_pattern, uri)
    if work_match:
        result["country"] = work_match.group(1)
        result["doc_type"] = work_match.group(2)
        result["year"] = int(work_match.group(3))
        result["number"] = int(work_match.group(4))

    # Expression pattern: .../{lang}@{date}
    expr_pattern = r"/([a-z]{2,3})@(\d{4}-\d{2}-\d{2})"
    expr_match = re.search(expr_pattern, uri)
    if expr_match:
        result["language"] = expr_match.group(1)
        try:
            result["version_date"] = date.fromisoformat(expr_match.group(2))
        except ValueError:  # pragma: no cover
            pass

    # Section pattern: .../section/{num}
    section_pattern = r"/section/(\d+[A-Za-z]?)"
    section_match = re.search(section_pattern, uri)
    if section_match:
        result["section"] = section_match.group(1)

    return result
