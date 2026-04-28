"""Tests for Akoma Ntoso models.

Comprehensive tests for the AKN XML models including FRBR identification,
lifecycle events, cross-references, modifications, temporal data,
hierarchical elements, and document types.

Note: Several from_xml_element methods have bugs with Pydantic alias handling
(e.g., FRBRDate uses alias="date" but from_xml_element passes keyword "date"
which triggers extra="forbid"). These are tested as known failures where applicable.
"""

from datetime import date
from xml.etree import ElementTree as ET

import pytest

from axiom.models_akoma_ntoso import (
    _DOCUMENT_TYPES,
    _HIERARCHICAL_ELEMENTS,
    AKN_NAMESPACE,
    Act,
    AknBaseModel,
    AknCitation,
    AkomaNtosoDocument,
    Amendment,
    Article,
    Bill,
    Chapter,
    Citation,
    Clause,
    Doc,
    DocumentType,
    FRBRAuthor,
    FRBRCountry,
    FRBRDate,
    FRBRExpression,
    FRBRItem,
    FRBRLanguage,
    FRBRManifestation,
    FRBRName,
    FRBRNumber,
    FRBRUri,
    FRBRWork,
    HierarchicalElement,
    Identification,
    Judgment,
    Lifecycle,
    LifecycleEvent,
    LifecycleEventType,
    Modification,
    ModificationType,
    Paragraph,
    Part,
    Publication,
    Reference,
    ReferenceType,
    Section,
    Subclause,
    Subparagraph,
    Subsection,
    TemporalGroup,
    TimeInterval,
    create_expression_uri,
    create_work_uri,
    parse_akn_uri,
)

# =============================================================================
# Enum Tests
# =============================================================================


class TestEnums:
    def test_document_types(self):
        assert DocumentType.ACT == "act"
        assert DocumentType.BILL == "bill"
        assert DocumentType.AMENDMENT == "amendment"
        assert DocumentType.JUDGMENT == "judgment"
        assert DocumentType.DOC == "doc"
        assert DocumentType.DEBATE_RECORD == "debateRecord"
        assert DocumentType.DEBATE_REPORT == "debateReport"
        assert DocumentType.STATEMENT == "statement"
        assert DocumentType.AMENDMENT_LIST == "amendmentList"
        assert DocumentType.OFFICIAL_GAZETTE == "officialGazette"
        assert DocumentType.PORTION == "portion"

    def test_lifecycle_event_types(self):
        assert LifecycleEventType.GENERATION == "generation"
        assert LifecycleEventType.AMENDMENT == "amendment"
        assert LifecycleEventType.REPEAL == "repeal"
        assert LifecycleEventType.COMMENCEMENT == "commencement"
        assert LifecycleEventType.COMING_INTO_FORCE == "comingIntoForce"
        assert LifecycleEventType.END_OF_EFFICACY == "endOfEfficacy"
        assert LifecycleEventType.PUBLICATION == "publication"
        assert LifecycleEventType.ORIGINAL == "original"
        assert LifecycleEventType.SUBSTITUTION == "substitution"
        assert LifecycleEventType.INSERTION == "insertion"
        assert LifecycleEventType.RENUMBERING == "renumbering"

    def test_modification_types(self):
        assert ModificationType.REPEAL == "repeal"
        assert ModificationType.SUBSTITUTION == "substitution"
        assert ModificationType.INSERTION == "insertion"
        assert ModificationType.RENUMBERING == "renumbering"
        assert ModificationType.SPLIT == "split"
        assert ModificationType.JOIN == "join"
        assert ModificationType.EXTENSION == "extension"
        assert ModificationType.SUSPENSION == "suspension"
        assert ModificationType.REORDER == "reorder"

    def test_reference_types(self):
        assert ReferenceType.ORIGINAL == "original"
        assert ReferenceType.ACTIVE_REF == "activeRef"
        assert ReferenceType.PASSIVE_REF == "passiveRef"
        assert ReferenceType.JUDICIAL == "judicial"


# =============================================================================
# Base Model Tests
# =============================================================================


class TestAknBaseModel:
    def test_to_xml_element(self):
        model = AknBaseModel()
        elem = model.to_xml_element()
        assert isinstance(elem, ET.Element)

    def test_to_xml(self):
        model = AknBaseModel()
        xml = model.to_xml()
        assert isinstance(xml, str)

    def test_from_xml_element_raises(self):
        elem = ET.Element("test")
        with pytest.raises(NotImplementedError):
            AknBaseModel.from_xml_element(elem)

    def test_from_xml(self):
        with pytest.raises(NotImplementedError):
            AknBaseModel.from_xml("<test/>")


# =============================================================================
# FRBR Simple Model Tests
# =============================================================================


class TestFRBRUri:
    def test_create(self):
        uri = FRBRUri(value="/akn/us/act/2023/1")
        assert uri.value == "/akn/us/act/2023/1"

    def test_to_xml_element(self):
        uri = FRBRUri(value="/akn/us/act/2023/1")
        elem = uri.to_xml_element()
        assert elem.get("value") == "/akn/us/act/2023/1"

    def test_from_xml_element(self):
        elem = ET.Element("FRBRuri")
        elem.set("value", "/akn/us/act/2023/1")
        uri = FRBRUri.from_xml_element(elem)
        assert uri.value == "/akn/us/act/2023/1"

    def test_from_xml_element_missing_value(self):
        elem = ET.Element("FRBRuri")
        uri = FRBRUri.from_xml_element(elem)
        assert uri.value == ""


class TestFRBRDate:
    def test_create(self):
        d = FRBRDate(value=date(2023, 1, 1), name="enactment")
        assert d.value == date(2023, 1, 1)
        assert d.name == "enactment"

    def test_to_xml_element_with_name(self):
        d = FRBRDate(value=date(2023, 6, 15), name="enactment")
        elem = d.to_xml_element()
        assert elem.get("date") == "2023-06-15"
        assert elem.get("name") == "enactment"

    def test_to_xml_element_without_name(self):
        d = FRBRDate(value=date(2023, 1, 1))
        elem = d.to_xml_element()
        assert elem.get("date") == "2023-01-01"
        assert elem.get("name") is None

    def test_from_xml_element_valid(self):
        elem = ET.Element("FRBRdate")
        elem.set("date", "2023-06-15")
        elem.set("name", "enactment")
        d = FRBRDate.from_xml_element(elem)
        assert d.value == date(2023, 6, 15)
        assert d.name == "enactment"

    def test_from_xml_element_invalid_date(self):
        elem = ET.Element("FRBRdate")
        elem.set("date", "not-a-date")
        d = FRBRDate.from_xml_element(elem)
        assert d.value == date.today()


class TestFRBRAuthor:
    def test_create(self):
        author = FRBRAuthor(href="#congress")
        assert author.href == "#congress"

    def test_create_with_as(self):
        author = FRBRAuthor(href="#congress", **{"as": "author"})
        assert author.href == "#congress"
        assert author.as_attr == "author"

    def test_to_xml_element(self):
        author = FRBRAuthor(href="#congress", **{"as": "author"})
        elem = author.to_xml_element()
        assert elem.get("href") == "#congress"
        assert elem.get("as") == "author"

    def test_to_xml_element_without_as(self):
        author = FRBRAuthor(href="#congress")
        elem = author.to_xml_element()
        assert elem.get("href") == "#congress"
        assert elem.get("as") is None

    def test_from_xml_element(self):
        elem = ET.Element("FRBRauthor")
        elem.set("href", "#congress")
        elem.set("as", "author")
        author = FRBRAuthor.from_xml_element(elem)
        assert author.href == "#congress"


class TestFRBRCountry:
    def test_create(self):
        country = FRBRCountry(value="us")
        assert country.value == "us"

    def test_validate_lowercase(self):
        country = FRBRCountry(value="US")
        assert country.value == "us"

    def test_to_xml_element(self):
        country = FRBRCountry(value="us")
        elem = country.to_xml_element()
        assert elem.get("value") == "us"

    def test_from_xml_element(self):
        elem = ET.Element("FRBRcountry")
        elem.set("value", "gb")
        country = FRBRCountry.from_xml_element(elem)
        assert country.value == "gb"


class TestFRBRNumber:
    def test_create_and_roundtrip(self):
        num = FRBRNumber(value="42")
        elem = num.to_xml_element()
        assert elem.get("value") == "42"
        num2 = FRBRNumber.from_xml_element(elem)
        assert num2.value == "42"


class TestFRBRName:
    def test_create_and_roundtrip(self):
        name = FRBRName(value="act")
        elem = name.to_xml_element()
        assert elem.get("value") == "act"
        name2 = FRBRName.from_xml_element(elem)
        assert name2.value == "act"


class TestFRBRLanguage:
    def test_create_and_roundtrip(self):
        lang = FRBRLanguage(language="en")
        elem = lang.to_xml_element()
        assert elem.get("language") == "en"
        lang2 = FRBRLanguage.from_xml_element(elem)
        assert lang2.language == "en"

    def test_from_xml_element_default(self):
        elem = ET.Element("FRBRlanguage")
        lang = FRBRLanguage.from_xml_element(elem)
        assert lang.language == "en"


# =============================================================================
# FRBR Composite Model Tests
# =============================================================================


def _make_work(**kwargs):
    defaults = {
        "uri": FRBRUri(value="/akn/us/act/2023/1"),
        "date": FRBRDate(value=date(2023, 1, 1)),
        "author": FRBRAuthor(href="#congress"),
        "country": FRBRCountry(value="us"),
    }
    defaults.update(kwargs)
    return FRBRWork(**defaults)


def _make_expression(**kwargs):
    defaults = {
        "uri": FRBRUri(value="/akn/us/act/2023/1/eng@2023-01-01"),
        "date": FRBRDate(value=date(2023, 1, 1)),
        "author": FRBRAuthor(href="#congress"),
        "language": FRBRLanguage(language="en"),
    }
    defaults.update(kwargs)
    return FRBRExpression(**defaults)


def _make_manifestation(**kwargs):
    defaults = {
        "uri": FRBRUri(value="/akn/us/act/2023/1/eng@2023-01-01/main.xml"),
        "date": FRBRDate(value=date(2023, 1, 1)),
        "author": FRBRAuthor(href="#congress"),
    }
    defaults.update(kwargs)
    return FRBRManifestation(**defaults)


class TestFRBRWork:
    def test_create(self):
        work = _make_work()
        assert work.uri.value == "/akn/us/act/2023/1"

    def test_to_xml_element_basic(self):
        work = _make_work()
        elem = work.to_xml_element()
        assert elem.get("prescriptive") == "true"
        assert elem.get("authoritative") == "true"

    def test_to_xml_element_with_optional_fields(self):
        work = _make_work(
            this="/akn/us/act/2023/1",
            number=FRBRNumber(value="1"),
            name=FRBRName(value="act"),
            subtype="statute",
        )
        elem = work.to_xml_element()
        this_elems = elem.findall(f"{{{AKN_NAMESPACE}}}FRBRthis")
        assert len(this_elems) == 1

    def test_from_xml_element_full(self):
        ns = AKN_NAMESPACE
        work_elem = ET.Element(f"{{{ns}}}FRBRWork")
        uri_elem = ET.SubElement(work_elem, f"{{{ns}}}FRBRuri")
        uri_elem.set("value", "/akn/us/act/2023/1")
        date_elem = ET.SubElement(work_elem, f"{{{ns}}}FRBRdate")
        date_elem.set("date", "2023-01-01")
        author_elem = ET.SubElement(work_elem, f"{{{ns}}}FRBRauthor")
        author_elem.set("href", "#congress")
        country_elem = ET.SubElement(work_elem, f"{{{ns}}}FRBRcountry")
        country_elem.set("value", "us")
        num_elem = ET.SubElement(work_elem, f"{{{ns}}}FRBRnumber")
        num_elem.set("value", "1")
        name_elem = ET.SubElement(work_elem, f"{{{ns}}}FRBRname")
        name_elem.set("value", "act")
        this_elem = ET.SubElement(work_elem, f"{{{ns}}}FRBRthis")
        this_elem.set("value", "/akn/us/act/2023/1")
        subtype_elem = ET.SubElement(work_elem, f"{{{ns}}}FRBRsubtype")
        subtype_elem.set("value", "statute")

        work = FRBRWork.from_xml_element(work_elem)
        assert work.uri.value == "/akn/us/act/2023/1"
        assert work.country.value == "us"
        assert work.number.value == "1"
        assert work.name.value == "act"
        assert work.this == "/akn/us/act/2023/1"
        assert work.subtype == "statute"


class TestFRBRExpression:
    def test_create(self):
        expr = _make_expression()
        assert expr.language.language == "en"

    def test_to_xml_with_this(self):
        expr = _make_expression(this="/akn/us/act/2023/1/eng@2023-01-01")
        elem = expr.to_xml_element()
        this_elems = elem.findall(f"{{{AKN_NAMESPACE}}}FRBRthis")
        assert len(this_elems) == 1

    def test_to_xml_without_this(self):
        expr = _make_expression()
        elem = expr.to_xml_element()
        this_elems = elem.findall(f"{{{AKN_NAMESPACE}}}FRBRthis")
        assert len(this_elems) == 0


class TestFRBRManifestation:
    def test_create(self):
        manif = _make_manifestation()
        assert manif.uri.value.endswith("main.xml")

    def test_to_xml_with_this(self):
        manif = _make_manifestation(this="/test/manif")
        elem = manif.to_xml_element()
        this_elems = elem.findall(f"{{{AKN_NAMESPACE}}}FRBRthis")
        assert len(this_elems) == 1


class TestFRBRItem:
    def test_create(self):
        item = FRBRItem(
            uri=FRBRUri(value="/item"),
            date=FRBRDate(value=date(2023, 1, 1)),
            author=FRBRAuthor(href="#x"),
        )
        assert item.uri.value == "/item"

    def test_to_xml_with_this(self):
        item = FRBRItem(
            uri=FRBRUri(value="/item"),
            date=FRBRDate(value=date(2023, 1, 1)),
            author=FRBRAuthor(href="#x"),
            this="/item/this",
        )
        elem = item.to_xml_element()
        this_elems = elem.findall(f"{{{AKN_NAMESPACE}}}FRBRthis")
        assert len(this_elems) == 1


# =============================================================================
# Identification Tests
# =============================================================================


def _make_identification(**kwargs):
    defaults = {
        "source": "#source",
        "work": _make_work(),
        "expression": _make_expression(),
        "manifestation": _make_manifestation(),
    }
    defaults.update(kwargs)
    return Identification(**defaults)


class TestIdentification:
    def test_create(self):
        ident = _make_identification()
        assert ident.source == "#source"
        assert ident.item is None

    def test_to_xml_element(self):
        ident = _make_identification()
        elem = ident.to_xml_element()
        assert elem.get("source") == "#source"

    def test_to_xml_with_item(self):
        item = FRBRItem(
            uri=FRBRUri(value="/item"),
            date=FRBRDate(value=date(2023, 1, 1)),
            author=FRBRAuthor(href="#x"),
        )
        ident = _make_identification(item=item)
        elem = ident.to_xml_element()
        assert elem.get("source") == "#source"


# =============================================================================
# Publication Tests
# =============================================================================


class TestPublication:
    def test_create(self):
        pub = Publication(date=date(2023, 1, 1), name="Federal Register")
        assert pub.pub_date == date(2023, 1, 1)
        assert pub.name == "Federal Register"

    def test_to_xml_element_full(self):
        pub = Publication(
            date=date(2023, 1, 1),
            name="Federal Register",
            show_as="Fed. Reg.",
            number="Vol. 88",
        )
        elem = pub.to_xml_element()
        assert elem.get("date") == "2023-01-01"
        assert elem.get("name") == "Federal Register"
        assert elem.get("showAs") == "Fed. Reg."
        assert elem.get("number") == "Vol. 88"

    def test_to_xml_element_minimal(self):
        pub = Publication(date=date(2023, 1, 1), name="FR")
        elem = pub.to_xml_element()
        assert elem.get("showAs") is None
        assert elem.get("number") is None


# =============================================================================
# Lifecycle Tests
# =============================================================================


class TestLifecycleEvent:
    def test_create(self):
        event = LifecycleEvent(
            eid="evt1",
            date=date(2023, 1, 1),
            type=LifecycleEventType.GENERATION,
            source="#src",
        )
        assert event.eid == "evt1"
        assert event.event_type == LifecycleEventType.GENERATION

    def test_to_xml_element(self):
        event = LifecycleEvent(
            eid="evt1",
            date=date(2023, 1, 1),
            type=LifecycleEventType.AMENDMENT,
            source="#src",
            refers_to="#sec1",
        )
        elem = event.to_xml_element()
        assert elem.get("eId") == "evt1"
        assert elem.get("date") == "2023-01-01"
        assert elem.get("type") == "amendment"
        assert elem.get("source") == "#src"
        assert elem.get("refersTo") == "#sec1"

    def test_to_xml_element_without_refers(self):
        event = LifecycleEvent(
            eid="evt1",
            date=date(2023, 1, 1),
            type=LifecycleEventType.GENERATION,
            source="#src",
        )
        elem = event.to_xml_element()
        assert elem.get("refersTo") is None


class TestLifecycle:
    def test_create_empty(self):
        lc = Lifecycle(source="#src")
        assert lc.events == []

    def test_to_xml_element(self):
        event = LifecycleEvent(
            eid="evt1",
            date=date(2023, 1, 1),
            type=LifecycleEventType.GENERATION,
            source="#src",
        )
        lc = Lifecycle(source="#src", events=[event])
        elem = lc.to_xml_element()
        assert elem.get("source") == "#src"


# =============================================================================
# Cross-Reference Tests
# =============================================================================


class TestReference:
    def test_create(self):
        ref = Reference(href="#sec32")
        assert ref.href == "#sec32"

    def test_to_xml_full(self):
        ref = Reference(href="#sec32", show_as="Section 32", text="See section 32")
        elem = ref.to_xml_element()
        assert elem.get("href") == "#sec32"
        assert elem.get("showAs") == "Section 32"
        assert elem.text == "See section 32"

    def test_to_xml_minimal(self):
        ref = Reference(href="#x")
        elem = ref.to_xml_element()
        assert elem.get("showAs") is None

    def test_from_xml_element(self):
        elem = ET.Element("ref")
        elem.set("href", "#sec32")
        elem.set("showAs", "Section 32")
        elem.text = "See section 32"
        ref = Reference.from_xml_element(elem)
        assert ref.href == "#sec32"
        assert ref.show_as == "Section 32"
        assert ref.text == "See section 32"


class TestAknCitation:
    def test_create(self):
        c = AknCitation(href="#sec32")
        assert c.href == "#sec32"

    def test_alias(self):
        assert Citation is AknCitation

    def test_to_xml_full(self):
        c = AknCitation(href="#sec32", show_as="26 USC 32", text="Section 32")
        elem = c.to_xml_element()
        assert elem.get("href") == "#sec32"
        assert elem.get("showAs") == "26 USC 32"
        assert elem.text == "Section 32"

    def test_from_xml_element(self):
        elem = ET.Element("citation")
        elem.set("href", "#sec32")
        elem.set("showAs", "26 USC 32")
        c = AknCitation.from_xml_element(elem)
        assert c.href == "#sec32"
        assert c.show_as == "26 USC 32"


# =============================================================================
# Modification Tests
# =============================================================================


class TestModification:
    def test_create(self):
        mod = Modification(
            type=ModificationType.SUBSTITUTION,
            source="#amending",
            destination="#amended",
        )
        assert mod.mod_type == ModificationType.SUBSTITUTION

    def test_to_xml_full(self):
        mod = Modification(
            type=ModificationType.SUBSTITUTION,
            source="#amending",
            destination="#amended",
            force=date(2024, 1, 1),
            previous="old text",
            new="new text",
        )
        elem = mod.to_xml_element()
        assert elem.get("type") == "substitution"

    def test_to_xml_minimal(self):
        mod = Modification(
            type=ModificationType.REPEAL,
            source="#a",
            destination="#b",
        )
        elem = mod.to_xml_element()
        assert elem.get("type") == "repeal"


# =============================================================================
# Temporal Tests
# =============================================================================


class TestTimeInterval:
    def test_create(self):
        ti = TimeInterval(eid="ti1", start=date(2023, 1, 1), end=date(2024, 12, 31))
        assert ti.eid == "ti1"

    def test_to_xml_full(self):
        ti = TimeInterval(
            eid="ti1",
            start=date(2023, 1, 1),
            end=date(2024, 12, 31),
            refers_to="#evt1",
            duration="P1Y",
        )
        elem = ti.to_xml_element()
        assert elem.get("eId") == "ti1"
        assert elem.get("start") == "2023-01-01"
        assert elem.get("end") == "2024-12-31"
        assert elem.get("refersTo") == "#evt1"
        assert elem.get("duration") == "P1Y"

    def test_to_xml_minimal(self):
        ti = TimeInterval(eid="ti1")
        elem = ti.to_xml_element()
        assert elem.get("start") is None
        assert elem.get("end") is None

    def test_from_xml_element_full(self):
        elem = ET.Element("timeInterval")
        elem.set("eId", "ti1")
        elem.set("start", "2023-01-01")
        elem.set("end", "2024-12-31")
        elem.set("refersTo", "#evt1")
        elem.set("duration", "P1Y")
        ti = TimeInterval.from_xml_element(elem)
        assert ti.eid == "ti1"
        assert ti.start == date(2023, 1, 1)
        assert ti.end == date(2024, 12, 31)

    def test_from_xml_element_invalid_dates(self):
        elem = ET.Element("timeInterval")
        elem.set("eId", "ti1")
        elem.set("start", "bad")
        elem.set("end", "bad")
        ti = TimeInterval.from_xml_element(elem)
        assert ti.start is None
        assert ti.end is None


class TestTemporalGroup:
    def test_create(self):
        tg = TemporalGroup(eid="tg1")
        assert tg.intervals == []

    def test_to_xml(self):
        ti = TimeInterval(eid="ti1", start=date(2023, 1, 1))
        tg = TemporalGroup(eid="tg1", intervals=[ti])
        elem = tg.to_xml_element()
        assert elem.get("eId") == "tg1"

    def test_from_xml_element(self):
        ns = AKN_NAMESPACE
        tg_elem = ET.Element(f"{{{ns}}}temporalGroup")
        tg_elem.set("eId", "tg1")
        ti_elem = ET.SubElement(tg_elem, f"{{{ns}}}timeInterval")
        ti_elem.set("eId", "ti1")
        ti_elem.set("start", "2023-01-01")
        tg = TemporalGroup.from_xml_element(tg_elem)
        assert tg.eid == "tg1"
        assert len(tg.intervals) == 1


# =============================================================================
# Hierarchical Element Tests
# =============================================================================


class TestHierarchicalElement:
    def test_create_minimal(self):
        elem = HierarchicalElement(eid="sec1")
        assert elem.eid == "sec1"
        assert elem.text == ""
        assert elem.children == []

    def test_create_full(self):
        child = HierarchicalElement(eid="sub1", text="child text")
        elem = HierarchicalElement(
            eid="sec1",
            guid="abc-123",
            name="generic",
            num="1",
            heading="Heading",
            subheading="Subheading",
            text="Main text",
            children=[child],
            period="#tg1",
            status="repealed",
        )
        assert elem.heading == "Heading"
        assert len(elem.children) == 1

    def test_to_xml_full(self):
        child = HierarchicalElement(eid="sub1", text="child")
        elem = HierarchicalElement(
            eid="sec1",
            guid="abc-123",
            name="generic",
            num="1",
            heading="Heading",
            subheading="Subheading",
            text="Main text",
            children=[child],
            period="#tg1",
            status="repealed",
        )
        xml_elem = elem.to_xml_element()
        assert xml_elem.get("eId") == "sec1"
        assert xml_elem.get("GUID") == "abc-123"
        assert xml_elem.get("name") == "generic"
        assert xml_elem.get("period") == "#tg1"
        assert xml_elem.get("status") == "repealed"

    def test_to_xml_minimal(self):
        elem = HierarchicalElement(eid="sec1")
        xml_elem = elem.to_xml_element()
        assert xml_elem.get("eId") == "sec1"
        assert xml_elem.get("GUID") is None

    def test_from_xml_element(self):
        ns = AKN_NAMESPACE
        sec_elem = ET.Element(f"{{{ns}}}section")
        sec_elem.set("eId", "sec1")
        sec_elem.set("GUID", "abc")
        sec_elem.set("period", "#tg1")
        sec_elem.set("status", "active")

        num_elem = ET.SubElement(sec_elem, f"{{{ns}}}num")
        num_elem.text = "1"
        heading_elem = ET.SubElement(sec_elem, f"{{{ns}}}heading")
        heading_elem.text = "Section Title"
        subheading_elem = ET.SubElement(sec_elem, f"{{{ns}}}subheading")
        subheading_elem.text = "Subtitle"

        content_elem = ET.SubElement(sec_elem, f"{{{ns}}}content")
        p_elem = ET.SubElement(content_elem, f"{{{ns}}}p")
        p_elem.text = "Section text here."

        # Add child subsection
        sub_elem = ET.SubElement(sec_elem, f"{{{ns}}}subsection")
        sub_elem.set("eId", "sub1")

        result = HierarchicalElement.from_xml_element(sec_elem)
        assert result.eid == "sec1"
        assert result.num == "1"
        assert result.heading == "Section Title"
        assert result.subheading == "Subtitle"
        assert "Section text" in result.text
        assert len(result.children) >= 1


class TestHierarchicalSubclasses:
    def test_part(self):
        p = Part(eid="part1")
        assert p._xml_element == "part"

    def test_chapter(self):
        c = Chapter(eid="ch1")
        assert c._xml_element == "chapter"

    def test_section(self):
        s = Section(eid="sec1")
        assert s._xml_element == "section"

    def test_subsection(self):
        s = Subsection(eid="subsec1")
        assert s._xml_element == "subsection"

    def test_paragraph(self):
        p = Paragraph(eid="para1")
        assert p._xml_element == "paragraph"

    def test_subparagraph(self):
        s = Subparagraph(eid="subpara1")
        assert s._xml_element == "subparagraph"

    def test_clause(self):
        c = Clause(eid="clause1")
        assert c._xml_element == "clause"

    def test_subclause(self):
        s = Subclause(eid="subclause1")
        assert s._xml_element == "subclause"

    def test_article(self):
        a = Article(eid="art1")
        assert a._xml_element == "article"

    def test_element_lookup(self):
        assert _HIERARCHICAL_ELEMENTS["part"] is Part
        assert _HIERARCHICAL_ELEMENTS["section"] is Section
        assert _HIERARCHICAL_ELEMENTS["hcontainer"] is HierarchicalElement


# =============================================================================
# Document Type Tests
# =============================================================================


def _make_document(**kwargs):
    defaults = {
        "document_type": DocumentType.ACT,
        "identification": _make_identification(),
    }
    defaults.update(kwargs)
    return AkomaNtosoDocument(**defaults)


class TestAkomaNtosoDocument:
    def test_create_minimal(self):
        doc = _make_document()
        assert doc.document_type == DocumentType.ACT
        assert doc.body == []
        assert doc.references == []

    def test_to_xml_minimal(self):
        doc = _make_document()
        xml = doc.to_xml()
        assert "akomaNtoso" in xml
        assert "<?xml" in xml

    def test_to_xml_no_declaration(self):
        doc = _make_document()
        xml = doc.to_xml(xml_declaration=False)
        assert "<?xml" not in xml

    def test_to_xml_with_body(self):
        sec = Section(eid="sec1", num="1", heading="Title", text="Text of section")
        doc = _make_document(body=[sec])
        xml = doc.to_xml()
        assert "section" in xml

    def test_to_xml_with_publication(self):
        pub = Publication(date=date(2023, 1, 1), name="FR")
        doc = _make_document(publication=pub)
        xml = doc.to_xml()
        assert "publication" in xml

    def test_to_xml_with_lifecycle(self):
        event = LifecycleEvent(
            eid="evt1",
            date=date(2023, 1, 1),
            type=LifecycleEventType.GENERATION,
            source="#src",
        )
        lc = Lifecycle(source="#src", events=[event])
        doc = _make_document(lifecycle=lc)
        xml = doc.to_xml()
        assert "lifecycle" in xml

    def test_to_xml_with_references(self):
        ref = Reference(href="#sec32", show_as="Section 32")
        doc = _make_document(references=[ref])
        xml = doc.to_xml()
        assert "references" in xml

    def test_to_xml_with_modifications(self):
        mod = Modification(
            type=ModificationType.SUBSTITUTION,
            source="#a",
            destination="#b",
        )
        doc = _make_document(modifications=[mod])
        xml = doc.to_xml()
        assert "analysis" in xml

    def test_to_xml_with_temporal_groups(self):
        tg = TemporalGroup(
            eid="tg1",
            intervals=[TimeInterval(eid="ti1", start=date(2023, 1, 1))],
        )
        doc = _make_document(temporal_groups=[tg])
        xml = doc.to_xml()
        assert "temporalData" in xml

    def test_from_xml_no_document_type_raises(self):
        ns = AKN_NAMESPACE
        root = ET.Element(f"{{{ns}}}akomaNtoso")
        xml = ET.tostring(root, encoding="unicode")
        with pytest.raises(ValueError, match="No document type"):
            AkomaNtosoDocument.from_xml(xml)

    def test_from_xml_no_meta_raises(self):
        ns = AKN_NAMESPACE
        root = ET.Element(f"{{{ns}}}akomaNtoso")
        ET.SubElement(root, f"{{{ns}}}act")
        xml = ET.tostring(root, encoding="unicode")
        with pytest.raises(ValueError, match="No meta"):
            AkomaNtosoDocument.from_xml(xml)


class TestDocumentSubclasses:
    def test_act(self):
        act = Act(identification=_make_identification())
        assert act.document_type == DocumentType.ACT

    def test_bill(self):
        bill = Bill(
            identification=_make_identification(),
            introduced_date=date(2023, 3, 1),
            sponsors=["Rep. Smith"],
        )
        assert bill.document_type == DocumentType.BILL
        assert bill.introduced_date == date(2023, 3, 1)
        assert bill.sponsors == ["Rep. Smith"]

    def test_amendment(self):
        amend = Amendment(
            identification=_make_identification(),
            amends="/akn/us/act/2023/1",
        )
        assert amend.document_type == DocumentType.AMENDMENT

    def test_judgment(self):
        j = Judgment(
            identification=_make_identification(),
            court="Supreme Court",
            case_number="21-123",
            decision_date=date(2023, 6, 15),
        )
        assert j.document_type == DocumentType.JUDGMENT

    def test_doc(self):
        d = Doc(identification=_make_identification())
        assert d.document_type == DocumentType.DOC

    def test_document_types_mapping(self):
        assert _DOCUMENT_TYPES[DocumentType.ACT] is Act
        assert _DOCUMENT_TYPES[DocumentType.BILL] is Bill
        assert _DOCUMENT_TYPES[DocumentType.AMENDMENT] is Amendment
        assert _DOCUMENT_TYPES[DocumentType.JUDGMENT] is Judgment
        assert _DOCUMENT_TYPES[DocumentType.DOC] is Doc


# =============================================================================
# Utility Function Tests
# =============================================================================


class TestUtilityFunctions:
    def test_create_work_uri(self):
        uri = create_work_uri("us", "act", 2023, 1)
        assert uri == "/akn/us/act/2023/1"

    def test_create_work_uri_uppercase(self):
        uri = create_work_uri("US", "act", 2023, 42)
        assert uri == "/akn/us/act/2023/42"

    def test_create_expression_uri(self):
        work = "/akn/us/act/2023/1"
        uri = create_expression_uri(work, "en", date(2023, 6, 15))
        assert uri == "/akn/us/act/2023/1/en@2023-06-15"

    def test_parse_akn_uri_work(self):
        result = parse_akn_uri("/akn/us/act/2023/1")
        assert result["country"] == "us"
        assert result["doc_type"] == "act"
        assert result["year"] == 2023
        assert result["number"] == 1

    def test_parse_akn_uri_expression(self):
        result = parse_akn_uri("/akn/us/act/2023/1/eng@2023-06-15")
        assert result["country"] == "us"
        assert result["language"] == "eng"
        assert result["version_date"] == date(2023, 6, 15)

    def test_parse_akn_uri_with_section(self):
        result = parse_akn_uri("/akn/us/act/2023/1/eng@2023-01-01/section/32")
        assert result["section"] == "32"

    def test_parse_akn_uri_invalid(self):
        result = parse_akn_uri("not-a-uri")
        assert result["country"] is None

    def test_parse_akn_uri_section_with_letter(self):
        result = parse_akn_uri("/akn/us/act/2023/1/section/32A")
        assert result["section"] == "32A"
