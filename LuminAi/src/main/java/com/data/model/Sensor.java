package com.data.model;

import jakarta.annotation.Nullable;
import jakarta.persistence.*;

import java.util.ArrayList;
import java.util.List;

@Entity
public class Sensor {
    @Id
    @GeneratedValue
    @Column(name = "s_id")
    private Long id;

    @Column(unique=true)
    private String name;
    private String unit;

    @ManyToOne
    @Nullable
    private Group group;

    @OneToMany
    private List<SensorData> values = new ArrayList<>();



    public Sensor(String name) {
        this.name = name;
    }

    public Sensor() {

    }

    public void addValue(final SensorData value){
        this.values.add(value);
        value.setDevice(this);
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Nullable
    public Group getGroup() {
        return group;
    }

    public void setGroup(@Nullable Group group) {
        this.group = group;
    }
}