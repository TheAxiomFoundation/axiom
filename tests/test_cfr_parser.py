"""Tests for CFR XML parser."""



# Sample XML fragments for testing
SAMPLE_SECTION_XML = """
<DIV8 N="§ 1.32-1" NODE="26:1.0.1.1.1.0.1.100" TYPE="SECTION">
<HEAD>§ 1.32-1   Earned income.</HEAD>
<P>(a) <I>In general.</I> For purposes of section 32, earned income means—
</P>
<P>(1) wages, salaries, tips, and other employee compensation, plus
</P>
<P>(2) the amount of the taxpayer's net earnings from self-employment.
</P>
<P>(b) <I>Special rules.</I> The following rules apply for purposes of this section:
</P>
<P>(1) <I>Combat pay.</I> A taxpayer may elect to include combat pay.
</P>
<CITA TYPE="N">[T.D. 9954, 86 FR 12345, Mar. 15, 2021]
</CITA>
</DIV8>
"""

SAMPLE_PART_XML = """
<DIV5 N="1" NODE="26:1.0.1.1.1" TYPE="PART">
<HEAD>PART 1—INCOME TAXES</HEAD>
<AUTH>
<HED>Authority:</HED><PSPACE>26 U.S.C. 7805, unless otherwise noted.
</PSPACE><P>Section 1.32-1 also issued under 26 U.S.C. 32;
</P>
</AUTH>
<DIV8 N="§ 1.32-1" NODE="26:1.0.1.1.1.0.1.100" TYPE="SECTION">
<HEAD>§ 1.32-1   Earned income.</HEAD>
<P>(a) <I>In general.</I> Test content.
</P>
</DIV8>
</DIV5>
"""


class TestCFRParser:
    """Tests for parsing CFR XML."""

    def test_parse_section_basic(self):
        """Parse a basic CFR section."""
        from axiom_corpus.parsers.cfr import parse_section

        section = parse_section(SAMPLE_SECTION_XML)
        assert section is not None
        assert section.citation.title == 26
        assert section.citation.part == 1
        assert section.citation.section == "32-1"
        assert section.heading == "Earned income"

    def test_parse_section_full_text(self):
        """Section full text is extracted."""
        from axiom_corpus.parsers.cfr import parse_section

        section = parse_section(SAMPLE_SECTION_XML)
        assert "earned income means" in section.full_text
        assert "wages, salaries, tips" in section.full_text

    def test_parse_section_subsections(self):
        """Subsections are parsed from paragraph structure."""
        from axiom_corpus.parsers.cfr import parse_section

        section = parse_section(SAMPLE_SECTION_XML)
        # Should have subsection (a) and (b)
        assert len(section.subsections) >= 2
        assert section.subsections[0].id == "a"
        assert "In general" in section.subsections[0].heading

    def test_parse_section_source(self):
        """Source citation is extracted from CITA element."""
        from axiom_corpus.parsers.cfr import parse_section

        section = parse_section(SAMPLE_SECTION_XML)
        assert "T.D. 9954" in section.source
        assert "86 FR 12345" in section.source

    def test_parse_section_node_path(self):
        """NODE attribute is used to derive citation."""
        from axiom_corpus.parsers.cfr import parse_section

        section = parse_section(SAMPLE_SECTION_XML)
        # NODE is "26:1.0.1.1.1.0.1.100" -> title 26
        assert section.citation.title == 26


class TestCFRPartParser:
    """Tests for parsing CFR parts with authority."""

    def test_parse_part_with_authority(self):
        """Parse part to extract authority statement."""
        from axiom_corpus.parsers.cfr import parse_part

        part = parse_part(SAMPLE_PART_XML)
        assert part is not None
        assert "26 U.S.C. 7805" in part["authority"]

    def test_parse_part_sections(self):
        """Parse all sections within a part."""
        from axiom_corpus.parsers.cfr import parse_part

        part = parse_part(SAMPLE_PART_XML)
        assert len(part["sections"]) >= 1
        assert part["sections"][0].citation.section == "32-1"


class TestCFRTitleParser:
    """Tests for parsing full CFR title XML."""

    def test_parse_title_metadata(self):
        """Parse title metadata from header."""
        from axiom_corpus.parsers.cfr import CFRParser

        # Minimal title XML
        title_xml = """<?xml version="1.0" encoding="UTF-8" ?>
        <DLPSTEXTCLASS>
        <HEADER>
        <FILEDESC>
        <TITLESTMT>
        <TITLE>Title 26: Internal Revenue</TITLE>
        </TITLESTMT>
        <PUBLICATIONSTMT>
        <IDNO TYPE="title">26</IDNO>
        </PUBLICATIONSTMT>
        </FILEDESC>
        </HEADER>
        <TEXT><BODY><ECFRBRWS>
        <AMDDATE>Dec. 18, 2025</AMDDATE>
        </ECFRBRWS></BODY></TEXT>
        </DLPSTEXTCLASS>
        """
        parser = CFRParser(title_xml)
        assert parser.title_number == 26
        assert parser.title_name == "Internal Revenue"

    def test_iterate_sections(self):
        """Iterate over all sections in a title."""
        from axiom_corpus.parsers.cfr import CFRParser

        # Title with one section
        title_xml = f"""<?xml version="1.0" encoding="UTF-8" ?>
        <DLPSTEXTCLASS>
        <HEADER>
        <FILEDESC>
        <TITLESTMT><TITLE>Title 26: Internal Revenue</TITLE></TITLESTMT>
        <PUBLICATIONSTMT><IDNO TYPE="title">26</IDNO></PUBLICATIONSTMT>
        </FILEDESC>
        </HEADER>
        <TEXT><BODY><ECFRBRWS>
        <DIV1 N="1" NODE="26:1" TYPE="TITLE">
        <DIV5 N="1" NODE="26:1.0.1.1.1" TYPE="PART">
        <HEAD>PART 1</HEAD>
        <AUTH><HED>Authority:</HED><PSPACE>26 U.S.C. 7805</PSPACE></AUTH>
        {SAMPLE_SECTION_XML}
        </DIV5>
        </DIV1>
        </ECFRBRWS></BODY></TEXT>
        </DLPSTEXTCLASS>
        """
        parser = CFRParser(title_xml)
        sections = list(parser.iter_sections())
        assert len(sections) >= 1
        assert sections[0].citation.section == "32-1"


class TestParagraphParsing:
    """Tests for parsing paragraph structure."""

    def test_extract_subsection_id(self):
        """Extract subsection ID from paragraph text."""
        from axiom_corpus.parsers.cfr import extract_subsection_id

        assert extract_subsection_id("(a) In general.") == "a"
        assert extract_subsection_id("(1) First item.") == "1"
        assert extract_subsection_id("(i) Roman numeral.") == "i"
        assert extract_subsection_id("No subsection here.") is None

    def test_extract_heading(self):
        """Extract heading from italic text."""
        from axiom_corpus.parsers.cfr import extract_heading

        assert extract_heading("(a) <I>In general.</I> The rule...") == "In general"
        assert extract_heading("(a) No heading here.") is None
