package bd24_project_a6_b.taxi_datastreaming.src.main.java.Dto;

public class AverageSpeedDistance {
    private String TaxiID;
    private double tripDistance;
    private double averageSpeed;

    public AverageSpeedDistance(String TaxiID, double tripDistance, double averageSpeed) {
        this.TaxiID = TaxiID;
        this.tripDistance = tripDistance;
        this.averageSpeed = averageSpeed;
    }

    // Getters and setters
    public String getTaxiID() {
        return TaxiID;
    }

    public void setTaxiID(String TaxiID) {
        this.TaxiID = TaxiID;
    }

    public double getTripDistance() {
        return tripDistance;
    }

    public void setTripDistance(double tripDistance) {
        this.tripDistance = tripDistance;
    }

    public double getAverageSpeed() {
        return averageSpeed;
    }

    public void setAverageSpeed(double averageSpeed) {
        this.averageSpeed = averageSpeed;
    }
}