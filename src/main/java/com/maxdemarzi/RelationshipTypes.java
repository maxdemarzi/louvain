package com.maxdemarzi;

import org.neo4j.graphdb.RelationshipType;

public enum RelationshipTypes implements RelationshipType
{
    BILL_AMOUNT,
    HAS_SPECIALTY,
    HAS_ADDRESS,
    SHARE_MEMBER
}
