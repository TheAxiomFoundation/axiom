"""Tests for CLML (Crown Legislation Markup Language) XML parser."""

from datetime import date

# Sample CLML XML fragments for testing
SAMPLE_SECTION_XML = """<?xml version="1.0" encoding="UTF-8"?>
<Legislation xmlns="http://www.legislation.gov.uk/namespaces/legislation"
             xmlns:ukm="http://www.legislation.gov.uk/namespaces/metadata"
             xmlns:dc="http://purl.org/dc/elements/1.1/"
             DocumentURI="http://www.legislation.gov.uk/ukpga/2003/1/section/62">
<ukm:Metadata>
    <dc:title>Income Tax (Earnings and Pensions) Act 2003</dc:title>
    <ukm:PrimaryMetadata>
        <ukm:DocumentClassification>
            <ukm:DocumentCategory Value="primary"/>
            <ukm:DocumentMainType Value="UnitedKingdomPublicGeneralAct"/>
        </ukm:DocumentClassification>
        <ukm:Year Value="2003"/>
        <ukm:Number Value="1"/>
    </ukm:PrimaryMetadata>
    <ukm:EnactmentDate Date="2003-04-10"/>
</ukm:Metadata>
<Primary>
    <Body>
        <P1 id="section-62">
            <Pnumber>62</Pnumber>
            <P1para>
                <Text><Term id="term-earnings">"Earnings"</Term>, in the case of an employment, means—</Text>
                <P2 id="section-62-a">
                    <Pnumber>a</Pnumber>
                    <P2para>
                        <Text>any salary, wages or fee,</Text>
                    </P2para>
                </P2>
                <P2 id="section-62-b">
                    <Pnumber>b</Pnumber>
                    <P2para>
                        <Text>any gratuity or other profit or incidental benefit of any kind obtained by the employee if it is money or money's worth, or</Text>
                    </P2para>
                </P2>
                <P2 id="section-62-c">
                    <Pnumber>c</Pnumber>
                    <P2para>
                        <Text>anything else that constitutes an emolument of the employment.</Text>
                    </P2para>
                </P2>
            </P1para>
        </P1>
    </Body>
</Primary>
</Legislation>
"""

SAMPLE_ACT_METADATA_XML = """<?xml version="1.0" encoding="UTF-8"?>
<Legislation xmlns="http://www.legislation.gov.uk/namespaces/legislation"
             xmlns:ukm="http://www.legislation.gov.uk/namespaces/metadata"
             xmlns:dc="http://purl.org/dc/elements/1.1/"
             DocumentURI="http://www.legislation.gov.uk/ukpga/2003/1"
             NumberOfProvisions="725">
<ukm:Metadata>
    <dc:title>Income Tax (Earnings and Pensions) Act 2003</dc:title>
    <dc:description>An Act to restate, with minor changes, certain enactments relating to income tax on employment income, pension income and social security income.</dc:description>
    <ukm:PrimaryMetadata>
        <ukm:DocumentClassification>
            <ukm:DocumentCategory Value="primary"/>
            <ukm:DocumentMainType Value="UnitedKingdomPublicGeneralAct"/>
        </ukm:DocumentClassification>
        <ukm:Year Value="2003"/>
        <ukm:Number Value="1"/>
    </ukm:PrimaryMetadata>
    <ukm:EnactmentDate Date="2003-04-10"/>
    <ukm:ComingIntoForce>
        <ukm:DateTime Date="2003-04-06"/>
    </ukm:ComingIntoForce>
</ukm:Metadata>
<Primary>
    <Body>
        <Part id="part-1">
            <Number>Part 1</Number>
            <Title>Overview</Title>
        </Part>
        <Part id="part-2">
            <Number>Part 2</Number>
            <Title>Employment income: charge to tax</Title>
        </Part>
    </Body>
</Primary>
</Legislation>
"""

SAMPLE_SECTION_WITH_AMENDMENT_XML = """<?xml version="1.0" encoding="UTF-8"?>
<Legislation xmlns="http://www.legislation.gov.uk/namespaces/legislation"
             xmlns:ukm="http://www.legislation.gov.uk/namespaces/metadata"
             xmlns:dc="http://purl.org/dc/elements/1.1/">
<ukm:Metadata>
    <dc:title>Income Tax (Earnings and Pensions) Act 2003</dc:title>
    <ukm:PrimaryMetadata>
        <ukm:Year Value="2003"/>
        <ukm:Number Value="1"/>
    </ukm:PrimaryMetadata>
    <ukm:EnactmentDate Date="2003-04-10"/>
</ukm:Metadata>
<Primary>
    <Body>
        <P1 id="section-7">
            <Pnumber>7</Pnumber>
            <P1para>
                <Text>
                    <Substitution ChangeId="c123" CommentaryRef="c12345">
                        employment income
                    </Substitution>
                </Text>
            </P1para>
        </P1>
    </Body>
</Primary>
<Commentaries>
    <Commentary id="c12345" Type="F">
        <Para>
            <Text>Words substituted by
                <Citation id="cite-1" URI="http://www.legislation.gov.uk/ukpga/2017/32">
                    Finance Act 2017 (c. 32)
                </Citation>
                , s. 5(2), with effect from 16.11.2017.
            </Text>
        </Para>
    </Commentary>
</Commentaries>
</Legislation>
"""


class TestCLMLParser:
    """Tests for parsing CLML section XML."""

    def test_parse_section_basic(self):
        """Parse a basic CLML section."""
        from axiom_corpus.parsers.clml import parse_section

        section = parse_section(SAMPLE_SECTION_XML)
        assert section is not None
        assert section.citation.type == "ukpga"
        assert section.citation.year == 2003
        assert section.citation.number == 1
        assert section.citation.section == "62"

    def test_parse_section_title(self):
        """Extract section heading/title."""
        from axiom_corpus.parsers.clml import parse_section

        section = parse_section(SAMPLE_SECTION_XML)
        # Section 62 defines "Earnings"
        assert "Earnings" in section.title or "62" in section.title

    def test_parse_section_text(self):
        """Extract full section text."""
        from axiom_corpus.parsers.clml import parse_section

        section = parse_section(SAMPLE_SECTION_XML)
        assert "salary" in section.text
        assert "wages" in section.text
        assert "money's worth" in section.text

    def test_parse_section_subsections(self):
        """Parse subsection structure."""
        from axiom_corpus.parsers.clml import parse_section

        section = parse_section(SAMPLE_SECTION_XML)
        # Should have subsections a, b, c
        assert len(section.subsections) >= 3
        ids = [s.id for s in section.subsections]
        assert "a" in ids or "62-a" in ids

    def test_parse_enacted_date(self):
        """Extract enactment date from metadata."""
        from axiom_corpus.parsers.clml import parse_section

        section = parse_section(SAMPLE_SECTION_XML)
        assert section.enacted_date == date(2003, 4, 10)

    def test_parse_section_source_url(self):
        """Extract source URL from DocumentURI."""
        from axiom_corpus.parsers.clml import parse_section

        section = parse_section(SAMPLE_SECTION_XML)
        assert section.source_url == "http://www.legislation.gov.uk/ukpga/2003/1/section/62"


class TestCLMLActParser:
    """Tests for parsing complete Act metadata."""

    def test_parse_act_metadata(self):
        """Parse Act-level metadata."""
        from axiom_corpus.parsers.clml import parse_act_metadata

        act = parse_act_metadata(SAMPLE_ACT_METADATA_XML)
        assert act.title == "Income Tax (Earnings and Pensions) Act 2003"
        assert act.citation.year == 2003
        assert act.citation.number == 1

    def test_parse_act_dates(self):
        """Parse enactment and commencement dates."""
        from axiom_corpus.parsers.clml import parse_act_metadata

        act = parse_act_metadata(SAMPLE_ACT_METADATA_XML)
        assert act.enacted_date == date(2003, 4, 10)
        assert act.commencement_date == date(2003, 4, 6)

    def test_parse_act_parts(self):
        """Parse parts structure."""
        from axiom_corpus.parsers.clml import parse_act_metadata

        act = parse_act_metadata(SAMPLE_ACT_METADATA_XML)
        assert len(act.parts) >= 2
        assert act.parts[0].title == "Overview"

    def test_parse_act_section_count(self):
        """Extract section count from NumberOfProvisions."""
        from axiom_corpus.parsers.clml import parse_act_metadata

        act = parse_act_metadata(SAMPLE_ACT_METADATA_XML)
        assert act.section_count == 725


class TestCLMLAmendmentParsing:
    """Tests for parsing amendment information."""

    def test_parse_amendments(self):
        """Parse amendment/substitution elements."""
        from axiom_corpus.parsers.clml import parse_section

        section = parse_section(SAMPLE_SECTION_WITH_AMENDMENT_XML)
        # Should detect the substitution
        assert len(section.amendments) >= 1

    def test_amendment_details(self):
        """Extract amendment details from commentary."""
        from axiom_corpus.parsers.clml import parse_section

        section = parse_section(SAMPLE_SECTION_WITH_AMENDMENT_XML)
        if section.amendments:
            amendment = section.amendments[0]
            assert amendment.type == "substitution"
            assert "2017" in amendment.amending_act


class TestCLMLTextExtraction:
    """Tests for text extraction utilities."""

    def test_extract_text_content(self):
        """Extract plain text from mixed content."""
        from axiom_corpus.parsers.clml import extract_text

        xml = "<Text>any <Term>salary</Term>, wages or fee,</Text>"
        text = extract_text(xml)
        assert "salary" in text
        assert "wages" in text
        assert "<" not in text  # No XML tags

    def test_normalize_whitespace(self):
        """Normalize whitespace in extracted text."""
        from axiom_corpus.parsers.clml import extract_text

        xml = "<Text>some    text\n\nwith   spaces</Text>"
        text = extract_text(xml)
        assert "  " not in text  # No double spaces


class TestCLMLCitationExtraction:
    """Tests for extracting cross-references."""

    def test_extract_citations(self):
        """Extract citation URIs from text."""
        from axiom_corpus.parsers.clml import extract_citations

        xml = SAMPLE_SECTION_WITH_AMENDMENT_XML
        citations = extract_citations(xml)
        assert any("ukpga/2017/32" in c for c in citations)


class TestCLMLExtentParsing:
    """Tests for territorial extent parsing."""

    def test_parse_extent(self):
        """Parse RestrictExtent attribute."""
        from axiom_corpus.parsers.clml import parse_extent

        # E+W+S+N.I. format
        extent = parse_extent("E+W+S+N.I.")
        assert "E" in extent
        assert "W" in extent
        assert "S" in extent
        assert "N.I." in extent

    def test_parse_extent_partial(self):
        """Parse partial extent."""
        from axiom_corpus.parsers.clml import parse_extent

        extent = parse_extent("E+W")
        assert len(extent) == 2
        assert "S" not in extent
