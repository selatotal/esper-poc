package br.com.selat.esperpoc.contract;

import java.util.Date;

public class TemperatureEvent {

    private double temperature;
    private Date timeOfReading;

    public TemperatureEvent() {
    }

    public TemperatureEvent(double temperature, Date timeOfReading) {
        this.temperature = temperature;
        this.timeOfReading = timeOfReading;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public Date getTimeOfReading() {
        return timeOfReading;
    }

    public void setTimeOfReading(Date timeOfReading) {
        this.timeOfReading = timeOfReading;
    }

    @Override
    public String toString() {
        return "TemperatureEvent{" +
                "temperature=" + temperature +
                ", timeOfReading=" + timeOfReading +
                '}';
    }
}
