"""Tests for NZ PCO legislation converter."""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from axiom_corpus.converters.nz_pco import (
    NZLegislation,
    NZPCOConverter,
)

SAMPLE_NZ_ACT_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<act id="DLM407935" year="2007" act.no="97" act.type="public"
     date.assent="2007-11-15" stage="in-force" date.as.at="2024-06-01">
  <cover>
    <title>Income Tax Act 2007</title>
    <assent>2007-11-15</assent>
  </cover>
  <ministry>Inland Revenue</ministry>
  <long-title>An Act to consolidate and reform the law relating to income tax</long-title>
  <body>
    <prov id="DLM407936">
      <label>1</label>
      <heading>Title</heading>
      <prov.body>
        <subprov id="DLM407937">
          <label>(1)</label>
          <para><text>This Act is the Income Tax Act 2007.</text></para>
        </subprov>
        <subprov id="DLM407938">
          <label>(2)</label>
          <para><text>This Act comes into force on 1 April 2008.</text></para>
        </subprov>
      </prov.body>
    </prov>
    <prov id="DLM407939">
      <label>2</label>
      <heading>Interpretation</heading>
      <prov.body>
        <para>
          <text>In this Act, the following terms have the meanings given.</text>
          <label-para>
            <label>(a)</label>
            <text>amount means any amount of money</text>
          </label-para>
          <label-para>
            <label>(b)</label>
            <text>tax means income tax</text>
          </label-para>
        </para>
      </prov.body>
    </prov>
    <prov id="DLM407940">
      <label>3</label>
      <heading>Empty section</heading>
    </prov>
  </body>
</act>
"""

SAMPLE_BILL_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<bill id="DLM123456" year="2024" bill.no="42" bill.type="government">
  <cover>
    <title>Tax Simplification Bill</title>
  </cover>
  <body>
    <prov id="DLM123457">
      <label>1</label>
      <heading>Title</heading>
      <prov.body>
        <subprov>
          <label>(1)</label>
          <para><text>This Act is the Tax Simplification Act.</text></para>
        </subprov>
      </prov.body>
    </prov>
  </body>
</bill>
"""

SAMPLE_REGULATION_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<regulation id="DLM200000" year="2020" regulation.no="10" regulation.type="public">
  <cover><title>Income Tax Regulations 2020</title></cover>
  <body></body>
</regulation>
"""

SAMPLE_ATOM_RSS = """\
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>NZ Legislation</title>
  <entry>
    <id>https://www.legislation.govt.nz/act/public/2007/0097/latest/contents.html</id>
    <title>Income Tax Act 2007</title>
    <published>2024-01-15T10:30:00Z</published>
    <updated>2024-06-01T08:00:00Z</updated>
    <content type="html">&lt;b&gt;Status:&lt;/b&gt; Modified</content>
  </entry>
</feed>
"""

SAMPLE_RSS2 = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <link>https://www.legislation.govt.nz/act/public/2007/0097/latest/contents.html</link>
      <title>Income Tax Act 2007</title>
      <pubDate>Mon, 15 Jan 2024 10:30:00 GMT</pubDate>
      <description>Updated</description>
    </item>
  </channel>
</rss>
"""


@pytest.fixture
def converter():
    c = NZPCOConverter.__new__(NZPCOConverter)
    c.timeout = 30
    c._client = None
    return c


class TestNZLegislationModel:
    def test_citation(self):
        leg = NZLegislation(
            id="DLM1", legislation_type="act", subtype="public",
            year=2007, number=97, title="Income Tax Act 2007",
        )
        assert "Income Tax Act 2007" in leg.citation
        assert "2007" in leg.citation

    def test_citation_sop(self):
        leg = NZLegislation(
            id="DLM1", legislation_type="sop", subtype="government",
            year=2024, number=42, title="SOP Title",
        )
        # SOP should use "Supplementary Order Paper" but citation includes title
        assert "SOP Title" in leg.citation

    def test_url(self):
        leg = NZLegislation(
            id="DLM1", legislation_type="act", subtype="public",
            year=2007, number=97, title="Income Tax Act 2007",
        )
        assert "legislation.govt.nz" in leg.url
        assert "2007" in leg.url
        assert "0097" in leg.url


class TestParseXml:
    def test_parse_act(self, converter):
        result = converter.parse_xml(SAMPLE_NZ_ACT_XML)
        assert result.legislation_type == "act"
        assert result.subtype == "public"
        assert result.year == 2007
        assert result.number == 97
        assert result.title == "Income Tax Act 2007"
        assert result.assent_date == date(2007, 11, 15)
        assert result.administering_ministry == "Inland Revenue"
        assert "income tax" in result.long_title.lower()

    def test_parse_provisions(self, converter):
        result = converter.parse_xml(SAMPLE_NZ_ACT_XML)
        assert len(result.provisions) >= 2
        assert result.provisions[0].label == "1"
        assert result.provisions[0].heading == "Title"

    def test_parse_subprovisions(self, converter):
        result = converter.parse_xml(SAMPLE_NZ_ACT_XML)
        sec1 = result.provisions[0]
        assert len(sec1.subprovisions) == 2
        assert sec1.subprovisions[0].label == "(1)"
        assert "Income Tax Act" in sec1.subprovisions[0].text

    def test_parse_label_paras(self, converter):
        result = converter.parse_xml(SAMPLE_NZ_ACT_XML)
        sec2 = result.provisions[1]
        assert len(sec2.paragraphs) >= 1

    def test_parse_bill(self, converter):
        result = converter.parse_xml(SAMPLE_BILL_XML)
        assert result.legislation_type == "bill"
        assert result.subtype == "government"
        assert result.year == 2024
        assert result.number == 42

    def test_parse_regulation(self, converter):
        result = converter.parse_xml(SAMPLE_REGULATION_XML)
        assert result.legislation_type == "regulation"
        assert result.year == 2020

    def test_unknown_root_defaults_to_act(self, converter):
        xml = '<unknown id="X" year="2020" act.no="1" act.type="public"><body></body></unknown>'
        result = converter.parse_xml(xml)
        assert result.legislation_type == "act"

    def test_empty_provision_skipped(self, converter):
        result = converter.parse_xml(SAMPLE_NZ_ACT_XML)
        # Section 3 has no prov.body, so it should still be included (has label+heading)
        labels = [p.label for p in result.provisions]
        assert "3" in labels

    def test_version_date(self, converter):
        result = converter.parse_xml(SAMPLE_NZ_ACT_XML)
        assert result.version_date == date(2024, 6, 1)


class TestParseFile:
    def test_parse_file(self, converter, tmp_path):
        xml_path = tmp_path / "test_act.xml"
        xml_path.write_text(SAMPLE_NZ_ACT_XML, encoding="utf-8")

        result = converter.parse_file(xml_path)
        assert result.title == "Income Tax Act 2007"

    def test_parse_file_str_path(self, converter, tmp_path):
        xml_path = tmp_path / "test_act.xml"
        xml_path.write_text(SAMPLE_NZ_ACT_XML, encoding="utf-8")

        result = converter.parse_file(str(xml_path))
        assert result.title == "Income Tax Act 2007"


class TestParseDate:
    def test_valid_date(self, converter):
        assert converter._parse_date("2024-01-15") == date(2024, 1, 15)

    def test_invalid_date(self, converter):
        assert converter._parse_date("bad-date") is None

    def test_none_input(self, converter):
        assert converter._parse_date(None) is None

    def test_empty_string(self, converter):
        assert converter._parse_date("") is None


class TestExtractTextRecursive:
    def test_plain_text(self, converter):
        from xml.etree import ElementTree as ET
        elem = ET.fromstring("<text>Hello world</text>")
        assert converter._extract_text_recursive(elem) == "Hello world"

    def test_nested_elements(self, converter):
        from xml.etree import ElementTree as ET
        elem = ET.fromstring("<text>Hello <b>world</b> text</text>")
        result = converter._extract_text_recursive(elem)
        assert "Hello" in result
        assert "world" in result

    def test_citation_element(self, converter):
        from xml.etree import ElementTree as ET
        xml = '<text>See <citation><atidlm:linkcontent xmlns:atidlm="http://www.arbortext.com/namespace/atidlm">section 32</atidlm:linkcontent></citation>.</text>'
        elem = ET.fromstring(xml)
        result = converter._extract_text_recursive(elem)
        assert "section 32" in result


class TestParseRss:
    def test_parse_atom(self, converter):
        items = converter.parse_rss(SAMPLE_ATOM_RSS)
        assert len(items) == 1
        assert items[0].title == "Income Tax Act 2007"
        assert items[0].legislation_type == "act"
        assert items[0].year == 2007
        assert items[0].status == "Modified"

    def test_parse_rss2(self, converter):
        items = converter.parse_rss(SAMPLE_RSS2)
        assert len(items) >= 1

    def test_parse_empty_feed(self, converter):
        xml = '<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom"></feed>'
        items = converter.parse_rss(xml)
        assert items == []


class TestContextManager:
    def test_context_manager(self):
        with patch.object(NZPCOConverter, "client", new_callable=lambda: property(lambda self: MagicMock())):
            with NZPCOConverter() as converter:
                assert converter is not None

    def test_close_without_client(self):
        converter = NZPCOConverter.__new__(NZPCOConverter)
        converter._client = None
        converter.close()  # Should not raise


class TestClientProperty:
    def test_lazy_client(self):
        with patch("axiom_corpus.converters.nz_pco.httpx.Client") as mock_cls:
            converter = NZPCOConverter(timeout=15)
            _ = converter.client
            mock_cls.assert_called_once()
