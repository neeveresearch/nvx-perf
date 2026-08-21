/**
 * Copyright 2016 Neeve Research, LLC
 *
 * This product includes software developed at Neeve Research, LLC
 * (http://www.neeveresearch.com/) as well as software licenced to
 * Neeve Research, LLC under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * Neeve Research licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.neeve.perf.serialization.rumi.xbuf2.random;

import java.nio.ByteBuffer;

import java.io.UnsupportedEncodingException;

import com.neeve.lang.XIntIterator;
import com.neeve.lang.XIterator;
import com.neeve.perf.serialization.Provider;
import com.neeve.perf.serialization.rumi.xbuf2.*;
import com.neeve.quark.QuarkBuffer;
import com.neeve.sma.MessageView;
import com.neeve.util.UtlTime;

public class CarBenchmark implements Provider<Car> {
    final private byte[] tempBuffer = new byte[128];
    final private int[] tempIntBuffer = new int[128];
    private static final byte[] MANUFACTURER;
    private static final byte[] MODEL;
    private static final byte[] ENG_MAN_CODE;
    private static final byte[] VEHICLE_CODE;
    private static final int[] SOME_NUMBERS;
    private static final OptionalExtras[] EXTRAS;
    static {
        try {
            MANUFACTURER = "MANUFACTURER".getBytes("UTF-8");
            MODEL = "MODEL".getBytes("UTF-8");
            ENG_MAN_CODE = "abc".getBytes("UTF-8");
            VEHICLE_CODE = "abcdef".getBytes("UTF-8");
            SOME_NUMBERS = new int[] { 0, 1, 2, 3, 4 };
            EXTRAS = new OptionalExtras[] { OptionalExtras.sportsPack, OptionalExtras.sunRoof };
        }
        catch (final UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }

    final private Car encodeCar = Car.create();
    final private Car decodeCar = Car.create();
    final private QuarkBuffer decodeBuffer = QuarkBuffer.createNative(1024);
    final private int decodeLength;
    private int encodedLength;
    {
        decodeLength = com.neeve.perf.serialization.rumi.xbuf2.serial.CarBenchmark.serializeTo(Car.Serializer.create(),
                                                                                               Engine.Serializer.create(),
                                                                                               PerformanceFigure.Serializer.create(),
                                                                                               FuelFigure.Serializer.create(),
                                                                                               Acceleration.Serializer.create(),
                                                                                               decodeBuffer,
                                                                                               QuarkBuffer.createNative(32),
                                                                                               QuarkBuffer.createNative(128));
    }

    private Car populate(final Car car) {
        final Car.Pojo pojo = car.pojo();
        pojo.setSerialNumber(12345);
        pojo.setModelYear((short)2005);
        pojo.setAvailable(BooleanType.T);
        pojo.setCode(Code.A);
        pojo.setVehicleCodeFrom(VEHICLE_CODE, 0, VEHICLE_CODE.length);

        pojo.setSomeNumbersFrom(SOME_NUMBERS, 0, SOME_NUMBERS.length);
        pojo.setExtrasFrom(EXTRAS, 0, EXTRAS.length);

        Engine engine = Engine.create();
        final Engine.Pojo epojo = engine.pojo();
        epojo.setCapacity((short)4200);
        epojo.setNumCylinders((byte)8);
        epojo.setManufacturerCodeFrom(ENG_MAN_CODE, 0, ENG_MAN_CODE.length);
        pojo.setEngine(engine);

        FuelFigure fuelFigure = FuelFigure.create();
        FuelFigure.Pojo fpojo = fuelFigure.pojo();
        fpojo.setSpeed((short)30);
        fpojo.setMpg(35.9f);
        pojo.addToFuelFigure(fuelFigure);
        fuelFigure = FuelFigure.create();
        fpojo = fuelFigure.pojo();
        fpojo.setSpeed((short)55);
        fpojo.setMpg(49.0f);
        pojo.addToFuelFigure(fuelFigure);
        fuelFigure = FuelFigure.create();
        fpojo = fuelFigure.pojo();
        fpojo.setSpeed((short)75);
        fpojo.setMpg(40.0f);
        pojo.addToFuelFigure(fuelFigure);

        PerformanceFigure performanceFigure = PerformanceFigure.create();
        PerformanceFigure.Pojo ppojo = performanceFigure.pojo();
        ppojo.setOctaneRating((byte)95);
        Acceleration acceleration = Acceleration.create();
        Acceleration.Pojo apojo = acceleration.pojo();
        apojo.setMph((short)30);
        apojo.setSeconds(4.0f);
        ppojo.addToAcceleration(acceleration);
        acceleration = Acceleration.create();
        apojo = acceleration.pojo();
        apojo.setMph((short)60);
        apojo.setSeconds(7.5f);
        ppojo.addToAcceleration(acceleration);
        acceleration = Acceleration.create();
        apojo = acceleration.pojo();
        apojo.setMph((short)100);
        apojo.setSeconds(12.2f);
        ppojo.addToAcceleration(acceleration);
        pojo.addToPerformanceFigure(performanceFigure);
        performanceFigure = PerformanceFigure.create();
        ppojo = performanceFigure.pojo();
        ppojo.setOctaneRating((byte)99);
        acceleration = Acceleration.create();
        apojo = acceleration.pojo();
        apojo.setMph((short)30);
        apojo.setSeconds(3.8f);
        ppojo.addToAcceleration(acceleration);
        acceleration = Acceleration.create();
        apojo = acceleration.pojo();
        apojo.setMph((short)60);
        apojo.setSeconds(7.1f);
        ppojo.addToAcceleration(acceleration);
        acceleration = Acceleration.create();
        apojo = acceleration.pojo();
        apojo.setMph((short)100);
        apojo.setSeconds(11.8f);
        ppojo.addToAcceleration(acceleration);
        pojo.addToPerformanceFigure(performanceFigure);

        pojo.setManufacturerFrom(MANUFACTURER, 0, MANUFACTURER.length);
        pojo.setModelFrom(MODEL, 0, MODEL.length);

        return car;
    }

    private void extract(final Car car) {
        final Car.Pojo pojo = car.pojo();
        pojo.getSerialNumber();
        pojo.getModelYear();
        pojo.getAvailable();
        pojo.getCode();
        pojo.getVehicleCode();

        final XIntIterator someNumbersIterator = pojo.getSomeNumbersIterator();
        while (someNumbersIterator.hasNext()) {
            someNumbersIterator.next();
        }
        final XIterator<OptionalExtras> extrasIterator = pojo.getExtrasIterator();
        while (extrasIterator.hasNext()) {
            extrasIterator.next();
        }

        Engine engine = pojo.getEngine();
        if (engine != null) {
            final Engine.Pojo epojo = engine.pojo();
            epojo.getCapacity();
            epojo.getNumCylinders();
            epojo.getManufacturerCode();
        }

        final XIterator<FuelFigure> fuelFigureIterator = pojo.getFuelFigureIterator();
        while (fuelFigureIterator.hasNext()) {
            final FuelFigure fuelFigure = fuelFigureIterator.next();
            final FuelFigure.Pojo fpojo = fuelFigure.pojo();
            fpojo.getSpeed();
            fpojo.getMpg();
        }

        final XIterator<PerformanceFigure> performanceFigureIterator = pojo.getPerformanceFigureIterator();
        while (performanceFigureIterator.hasNext()) {
            final PerformanceFigure performanceFigure = performanceFigureIterator.next();
            final PerformanceFigure.Pojo ppojo = performanceFigure.pojo();
            ppojo.getOctaneRating();
            final XIterator<Acceleration> accelerationIterator = ppojo.getAccelerationIterator();
            while (accelerationIterator.hasNext()) {
                final Acceleration acceleration = accelerationIterator.next();
                final Acceleration.Pojo apojo = acceleration.pojo();
                apojo.getMph();
                apojo.getSeconds();
            }
        }

        pojo.getManufacturer();
        pojo.getModel();
    }

    @Override
    public String name() {
        return "rumi.protobuf.random";
    }

    @Override
    public short vfid() {
        return com.neeve.perf.serialization.rumi.xbuf2.MessageFactory.VFID;
    }

    @Override
    public short otype() {
        return com.neeve.perf.serialization.rumi.xbuf2.MessageFactory.ID_Car;
    }
    @Override
    public int encoding() {
        return MessageView.ENCODING_TYPE_PROTOBUF;
    }

    @Override
    public Car create(final boolean encode) {
        final Car car = Car.create();
        car.setTimestamp(UtlTime.nowSinceEpoch());
        if (encode) {
            encode(car);
        }
        return car;
    }

    @Override
    public void prepareToEncode() {
        encodeCar.clear(false);
    }

    @Override
    public void encode(final Car car) {
        populate(car).sync();
        encodedLength = encodeCar.getSerializedBufferLength();
    }

    @Override
    public void encode() {
        populate(encodeCar).sync();
        encodedLength = encodeCar.getSerializedBufferLength();
    }

    @Override
    public int encodedLength() {
        return encodedLength;
    }

    @Override
    public void postEncode() {
    }

    @Override
    public void prepareToDecode() {
        decodeCar.clear(false);
    }

    @Override
    public void decode(final Car car) {
        extract(car);
    }

    @Override
    public void decode() {
        decodeBuffer.acquire();
        decodeCar.wrap(decodeBuffer, decodeLength);
        extract(decodeCar);
    }

    @Override
    public int decodedLength() {
        return decodeLength;
    }

    @Override
    public void postDecode() {
    }

    @Override
    public long dispose(final MessageView view) {
        final Car car = (Car)view;
        try {
            // note: the act of getting the timestamp will deserialize the entire payload
            //       if present i.e. of sender was configured to operate with encode=true
            return car.getTimestamp();
        }
        finally { 
            car.dispose();
        }
    }
}
