package Dto;

public class Transaction {

    private int TaxiID;
    private Timestamp timestamp;
    private double Latitude;
    private double Longitude;
    private int IsStarting;
    private int IsEnding;
}
public String getTaxiID() {
    return TaxiID;
}

public void setTaxiID(String TaxiID) {
    this.TaxiID = TaxiID;
}

public Timestamp getTimestamp() {
    return timestamp;
}

public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
}

public double getLatitude() {
    return Latitude;
}

public void setLatitude(double Latitude) {
    this.Latitude = Latitude;
}

public double getLongitude() {
    return Longitude;
}

public void setLongitude(double Longitude) {
    this.Longitude = Longitude;
}

public int IsStarting() {
    return IsStarting;
}

public void setStarting(boolean starting) {
    IsStarting = starting;
}

public boolean IsEnding() {
    return IsEnding;
}

public void setEnding(boolean ending) {
    IsEnding = ending;
}
}