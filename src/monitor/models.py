"""Data models for the auction monitoring system."""

from datetime import date
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


class Auction(BaseModel):
    """Represents an auction item from official sources."""

    # Identifiers
    source: str = Field(..., description="Official source of the auction data")
    external_id: Optional[str] = Field(
        None,
        description="Unique identifier from the source when available",
    )

    # Basic information
    title: str = Field(..., description="Title of the auction")
    province: str = Field(..., description="Province where the asset is located")
    municipality: str = Field(..., description="Municipality where the asset is located")
    postal_code: Optional[str] = Field(None, description="Postal code when it is safely available")

    # Asset classification
    asset_class: str = Field(..., description="Main class of the asset (e.g., real estate)")
    asset_subclass: str = Field(..., description="Subclass of the asset")
    is_vehicle: bool = Field(default=False, description="Whether the asset is a vehicle")

    # Status and dates
    official_status: str = Field(..., description="Current status of the auction")
    publication_date: Optional[date] = Field(None, description="Date when the auction was published")
    opening_date: Optional[date] = Field(None, description="Date when bidding opens")
    closing_date: Optional[date] = Field(None, description="Date when bidding closes")

    # Financial information
    appraisal_value: Optional[Decimal] = Field(None, description="Official appraisal value")
    starting_bid: Optional[Decimal] = Field(None, description="Starting bid amount")
    current_bid: Optional[Decimal] = Field(None, description="Current highest bid")
    deposit: Optional[Decimal] = Field(None, description="Required deposit amount")
    score: Optional[int] = Field(
        None,
        ge=0,
        le=100,
        description="Opportunity score from 0 to 100",
    )

    # Asset details
    occupancy_status: Optional[str] = Field(None, description="Occupancy status of the asset")
    encumbrances_summary: Optional[str] = Field(None, description="Summary of encumbrances")

    # Additional information
    description: Optional[str] = Field(None, description="Detailed description of the asset")
    official_url: Optional[str] = Field(None, description="URL to the official auction page")
