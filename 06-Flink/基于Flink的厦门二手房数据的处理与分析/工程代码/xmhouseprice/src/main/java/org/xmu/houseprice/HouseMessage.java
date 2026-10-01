package org.xmu.houseprice;

public class HouseMessage {
    private float UnitPrice;
    private float TotalPrice;
    private float BuildingArea;
    private String Title;
    private String URL;
    private String HuXing;
    private String LocationInfo;
    private String LocationInfoSec;
    private String CommunityName;
    private String ChaoXiang;
    private String ListingDay;
    private String LastTrade;
    private int Follower;
    private int BuildingYear;
    private int Elevator;
    private int ElevatorNum;
    private int HouseNum;
    private int HeightLocation;
    private int Height;
    private long timestamp;

    public String toString() {
        return "HouseMessage{" +
                ", Title=" + Title +
                ", URL=" + URL +
                ", unitPrice=" + UnitPrice +
                ", totalPrice=" + TotalPrice +
                ", follower=" + Follower +
                ", huXing='" + HuXing + '\'' +
                ", chaoXiang='" + ChaoXiang + '\'' +
                ", buildingArea=" + BuildingArea +
                ", buildingYear=" + BuildingYear +
                ", communityName='" + CommunityName + '\'' +
                ", locationInfo='" + LocationInfo + '\'' +
                ", locationInfoSec='" + LocationInfoSec + '\'' +
                ", elevator=" + Elevator +
                ", listingDay='" + ListingDay + '\'' +
                ", lastTrade='" + LastTrade + '\'' +
                ", heightLocation=" + HeightLocation +
                ", height=" + Height +
                ", elevatorNum=" + ElevatorNum +
                ", houseNum=" + HouseNum +
                '}';
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setURL(String URL) {
        this.URL = URL;
    }

    public String getURL() {
        return URL;
    }

    public void setTitle(String title) {
        Title = title;
    }

    public String getTitle() {
        return Title;
    }

    public void setHeight(int height) {
        Height = height;
    }

    public int getHeight() {
        return Height;
    }

    public void setHeightLocation(int heightLocation) {
        HeightLocation = heightLocation;
    }

    public int getHeightLocation() {
        return HeightLocation;
    }

    public void setHouseNum(int houseNum) {
        HouseNum = houseNum;
    }

    public int getHouseNum() {
        return HouseNum;
    }

    public void setElevatorNum(int elevatorNum) {
        ElevatorNum = elevatorNum;
    }

    public int getElevatorNum() {
        return ElevatorNum;
    }

    public void setElevator(int elevator) {
        Elevator = elevator;
    }

    public int getElevator() {
        return Elevator;
    }

    public void setLastTrade(String lastTrade) {
        LastTrade = lastTrade;
    }

    public String getLastTrade() {
        return LastTrade;
    }

    public void setListingDay(String listingDay) {
        ListingDay = listingDay;
    }

    public String getListingDay() {
        return ListingDay;
    }

    public void setCommunityName(String communityName) {
        CommunityName = communityName;
    }

    public String getCommunityName() {
        return CommunityName;
    }

    public void setChaoXiang(String chaoXiang) {
        ChaoXiang = chaoXiang;
    }

    public String getChaoXiang() {
        return ChaoXiang;
    }

    public void setLocationInfoSec(String locationInfoSec) {
        LocationInfoSec = locationInfoSec;
    }

    public String getLocationInfoSec() {
        return LocationInfoSec;
    }

    public void setLocationInfo(String locationInfo) {
        LocationInfo = locationInfo;
    }

    public String getLocationInfo() {
        return LocationInfo;
    }

    public void sethuXing(String huXing) {
        this.HuXing = huXing;
    }

    public String gethuXing() {
        return HuXing;
    }

    public void setBuildingYear(int buildingYear) {
        this.BuildingYear = buildingYear;
    }

    public int getBuildingYear() {
        return BuildingYear;
    }

    public void setBuildingArea(float buildingArea) {
        this.BuildingArea = buildingArea;
    }

    public float getBuildingArea() {
        return BuildingArea;
    }


    public void setFollower(int follower) {
        this.Follower = follower;
    }

    public int getFollower() {
        return Follower;
    }

    public void setUnitPrice(float unitPrice) {
        UnitPrice = unitPrice;
    }

    public float getUnitPrice() {
        return UnitPrice;
    }

    public void setTotalPrice(float totalPrice) {
        TotalPrice = totalPrice;
    }

    public float getTotalPrice() {
        return TotalPrice;
    }

    public String getCompositeKey() {
        return LocationInfo + "_" + LocationInfoSec;
    }

    public String getLocationCommunityKey() {
        return LocationInfo + "_" + CommunityName;
    }

    public double getHouseElevatordivide() {return (double) HouseNum / (double) ElevatorNum;}
}
