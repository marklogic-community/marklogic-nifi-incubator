<?xml version="1.0" encoding="UTF-8"?>
<xsl:transform xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:h="http://www.w3.org/1999/xhtml"
  xmlns:xd="http://www.oxygenxml.com/ns/doc/xsl"
  xmlns:xs="http://www.w3.org/2001/XMLSchema"
  exclude-result-prefixes="h xd xs"
  version="2.0">
  <xd:doc scope="stylesheet">
    <xd:desc>
      <xd:p><xd:b>Created on:</xd:b> Aug 8, 2018</xd:p>
      <xd:p><xd:b>Author:</xd:b> pfennell</xd:p>
      <xd:p>Constructs an event record.</xd:p>
    </xd:desc>
  </xd:doc>
  
  <xsl:output encoding="UTF-8" indent="yes" media-type="application/xml" method="xml"/>
  
  <xsl:strip-space elements="*"/>
  
  <xsl:param name="LATITUDE" as="xs:string" select="'0'"/>
  <xsl:param name="LONGITUDE" as="xs:string" select="'0'"/>
  <xsl:param name="LOCATION_ID" as="xs:string" select="'unknown'"/>
  <xsl:param name="EVENT_ID" as="xs:string" select="'unknown'"/>
  <xsl:variable name="BASE_URL" as="xs:string" select="'http://www.apnearanking.se/'"/>
  
  
  <xd:doc>
    <xd:desc></xd:desc>
  </xd:doc>
  <xsl:template match="/">
    <xsl:apply-templates select="h:tr"/>
  </xsl:template>


  <xd:doc>
    <xd:desc>construct the event document.</xd:desc>
  </xd:doc>
  <xsl:template match="h:tr">
    <xsl:variable name="organizer" as="xs:string?" select="normalize-space(substring-after(substring-before(@title, 'Judges:'), 'Organizer:'))"/>
    <xsl:variable name="judges" as="xs:string?" select="normalize-space(substring-after(substring-before(@title, 'Medic:'), 'Judges:'))"/>
    <xsl:variable name="medic" as="xs:string?" select="normalize-space(substring-after(@title, 'Medic:'))"/>
    <event id="{$EVENT_ID}" date="{h:td[1]}" type="{h:td[4]}">
      <source href="{$BASE_URL}{h:td[2]/h:a/@href}"/>
      <name><xsl:value-of select="replace(h:td[2]/h:a, '\n', ' ')"/></name>
      <location>
        <xsl:variable name="locationName" as="xs:string?" select="replace(h:td[3], '\n', ' ')"/>
        <xsl:if test="$locationName ne ','">
          <xsl:attribute name="lat" select="$LATITUDE"/>
          <xsl:attribute name="lng" select="$LONGITUDE"/>
          <xsl:attribute name="geonameId" select="$LOCATION_ID"/>
          <name><xsl:value-of select="$locationName"/></name>
        </xsl:if>
      </location>
      <organiser><xsl:value-of select="$organizer"/></organiser>
      <judges type="array">
        <xsl:for-each select="tokenize($judges, ',')">
          <xsl:if test="string-length(current()) gt 0 and not(matches(lower-case(current()), 'not set'))">
            <judge><xsl:value-of select="normalize-space(current())"/></judge>
          </xsl:if>
        </xsl:for-each>
      </judges>
      <xsl:if test="$medic ne '??'">
        <medic><xsl:value-of select="$medic"/></medic>
      </xsl:if>
      <disciplines>
        <xsl:apply-templates select="h:td" mode="disciplines"/>
      </disciplines>
    </event>
  </xsl:template>
  
  
  <xd:doc>
    <xd:desc>Determine disciplines for the event.</xd:desc>
  </xd:doc>
  <xsl:template match="h:td" mode="disciplines">
    <xsl:analyze-string select="string()" regex="&amp;nbsp([A-Z]{{3}})">
      <xsl:matching-substring>
        <xsl:attribute name="{lower-case(regex-group(1))}"><xsl:value-of select="true()"/></xsl:attribute>
      </xsl:matching-substring>
    </xsl:analyze-string>
  </xsl:template>
  
</xsl:transform>