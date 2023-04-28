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
package com.neeve.perf.serialization.rumi.xbuf2.serial;

import java.nio.ByteBuffer;

import java.io.UnsupportedEncodingException;

import com.neeve.perf.serialization.Provider;
import com.neeve.perf.serialization.rumi.xbuf2.*;
import com.neeve.sma.MessageView;
import com.neeve.quark.QuarkBuffer;
import com.neeve.quark.QuarkStringDeserializer;
import com.neeve.util.UtlTime;

public class CarBenchmark implements Provider<Car> {
    final private static class CarDeserializationCallback implements Car.Deserializer.Callback {
        final private byte[] tempBuffer = new byte[128];
        final private EngineDeserializationCallback engineDeserializationCallback = new EngineDeserializationCallback();
        final private FuelFigureDeserializationCallback fuelFigureDeserializationCallback = new FuelFigureDeserializationCallback();
        final private PerformanceFigureDeserializationCallback performanceFigureDeserializationCallback = new PerformanceFigureDeserializationCallback();
        long ts;

        @Override
        public void handleTimestamp(long val) {ts=val;}
        @Override
        public void handleSerialNumber(int val) { }
        @Override
        public void handleModelYear(short val) { }
        @Override
        public void handleAvailable(BooleanType val) { }
        @Override
        public void handleCode(Code val) { }
        @Override
        public void handleVehicleCode(QuarkStringDeserializer val) {  val.getTo(tempBuffer, 0);}
        @Override
        public void handleSomeNumbers(int val) { }
        @Override
        public void handleExtras(OptionalExtras val) { }
        @Override
        public void handleEngine(Engine.Deserializer val) { val.run(engineDeserializationCallback);}
        @Override
        public void handlePerformanceFigure(PerformanceFigure.Deserializer val) { val.run(performanceFigureDeserializationCallback);}
        @Override
        public void handleFuelFigure(FuelFigure.Deserializer val) { val.run(fuelFigureDeserializationCallback);}
        @Override
        public void handleManufacturer(QuarkStringDeserializer val) { val.getTo(tempBuffer, 0);}
        @Override
        public void handleModel(QuarkStringDeserializer val) { val.getTo(tempBuffer, 0);}
    }

    final private static class EngineDeserializationCallback implements Engine.Deserializer.Callback {
        final private byte[] tempBuffer = new byte[128];

        @Override
        public void handleCapacity(short val) { }
        @Override
        public void handleNumCylinders(byte val) { }
        @Override
        public void handleMaxRpm(short val) { } // Max RPM is supposed to be a constant and so processing it here with Rumi Protobuf would do more work than other encoding types (since Rumi Protobuf does not support constants)
        @Override
        public void handleManufacturerCode(QuarkStringDeserializer val) {
            val.getTo(tempBuffer, 0);
        }
        @Override
        public void handleFuel(QuarkStringDeserializer val) { } // Fuel is supposed to be a constant and so processing it here with Rumi Protobuf would do more work than other encoding types (since Rumi Protobuf does not support constants)
    }

    final private static class FuelFigureDeserializationCallback implements FuelFigure.Deserializer.Callback {
        @Override
        public void handleSpeed(short val) { }
        @Override
        public void handleMpg(float val) { }
    }

    final private static class PerformanceFigureDeserializationCallback implements PerformanceFigure.Deserializer.Callback {
        final private AccelerationDeserializationCallback accelerationDeserializationCallback = new AccelerationDeserializationCallback();

        @Override
        final public void handleOctaneRating(byte val) { }
        @Override
        final public void handleAcceleration(Acceleration.Deserializer val) { val.run(accelerationDeserializationCallback);}
    }

    final private static class AccelerationDeserializationCallback implements Acceleration.Deserializer.Callback {
        @Override
        public void handleMph(short val) { }
        @Override
        public void handleSeconds(float val) { }
    }

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

    final private Car.Serializer carSerializer = Car.Serializer.create();
    final private Car.Deserializer carDeserializer = Car.Deserializer.create();
    final private Engine.Serializer engineSerializer = Engine.Serializer.create();
    final private PerformanceFigure.Serializer performanceFigureSerializer = PerformanceFigure.Serializer.create();
    final private FuelFigure.Serializer fuelFigureSerializer = FuelFigure.Serializer.create();
    final private Acceleration.Serializer accelerationSerializer = Acceleration.Serializer.create();
    final private CarDeserializationCallback cb = new CarDeserializationCallback();
    final private QuarkBuffer carEncodeBuffer = QuarkBuffer.createNative(1024);
    private int carEncodedLength;
    final private QuarkBuffer carDecodeBuffer = QuarkBuffer.createNative(1024);
    final private int carDecodeLength;
    final private QuarkBuffer tempBuffer1 = QuarkBuffer.createNative(32);
    final private QuarkBuffer tempBuffer2 = QuarkBuffer.createNative(128);
    {
        carDecodeLength = serializeTo(carDecodeBuffer);
    }

    final private static int serializeTo(final Car.Serializer carSerializer, // already initialized with serialization buffer
                                         final Engine.Serializer engineSerializer,
                                         final PerformanceFigure.Serializer performanceFigureSerializer,
                                         final FuelFigure.Serializer fuelFigureSerializer,
                                         final Acceleration.Serializer accelerationSerializer,
                                         final QuarkBuffer tempBuffer1,
                                         final QuarkBuffer tempBuffer2) {
        carSerializer.serialNumber(12345)
            .modelYear((short)2005)
            .available(BooleanType.T)
            .code(Code.A)
            .vehicleCode(VEHICLE_CODE);

        for (int i  = 0; i < SOME_NUMBERS.length; i++) {
            carSerializer.someNumbers(SOME_NUMBERS[i]);
        }
        for (int i  = 0; i < EXTRAS.length; i++) {
            carSerializer.extras(EXTRAS[i]);
        }

        engineSerializer.init(tempBuffer1)
            .capacity((short)4200)
            .numCylinders((byte)8)
            .manufacturerCode(ENG_MAN_CODE)
            .done();
        carSerializer.engine(engineSerializer);

        fuelFigureSerializer.init(tempBuffer1)
            .speed((short)30)
            .mpg(35.9f)
            .done();
        carSerializer.fuelFigure(fuelFigureSerializer);
        fuelFigureSerializer.init(tempBuffer1)
            .speed((short)55)
            .mpg(49.0f)
            .done();
        carSerializer.fuelFigure(fuelFigureSerializer);
        fuelFigureSerializer.init(tempBuffer1)
            .speed((short)75)
            .mpg(40.0f)
            .done();
        carSerializer.fuelFigure(fuelFigureSerializer);

        performanceFigureSerializer.init(tempBuffer1).octaneRating((byte)95);
        accelerationSerializer.init(tempBuffer2)
            .mph((short)30)
            .seconds(4.0f)
            .done();
        performanceFigureSerializer.acceleration(accelerationSerializer);
        accelerationSerializer.init(tempBuffer2)
            .mph((short)60)
            .seconds(7.5f)
            .done();
        performanceFigureSerializer.acceleration(accelerationSerializer);
        accelerationSerializer.init(tempBuffer2)
            .mph((short)100)
            .seconds(12.2f)
            .done();
        performanceFigureSerializer.acceleration(accelerationSerializer);
        performanceFigureSerializer.done();
        carSerializer.performanceFigure(performanceFigureSerializer);

        performanceFigureSerializer.init(tempBuffer1).octaneRating((byte)99);
        accelerationSerializer.init(tempBuffer2)
            .mph((short)30)
            .seconds(3.8f)
            .done();
        performanceFigureSerializer.acceleration(accelerationSerializer);
        accelerationSerializer.init(tempBuffer2)
            .mph((short)60)
            .seconds(7.1f)
            .done();
        performanceFigureSerializer.acceleration(accelerationSerializer);
        accelerationSerializer.init(tempBuffer2)
            .mph((short)100)
            .seconds(11.8f)
            .done();
        performanceFigureSerializer.acceleration(accelerationSerializer);
        performanceFigureSerializer.done();
        carSerializer.performanceFigure(performanceFigureSerializer);

        return carSerializer.manufacturer(MANUFACTURER)
               .model(MODEL)
               .done();
    }

    final public static int serializeTo(final Car.Serializer carSerializer,
                                        final Engine.Serializer engineSerializer,
                                        final PerformanceFigure.Serializer performanceFigureSerializer,
                                        final FuelFigure.Serializer fuelFigureSerializer,
                                        final Acceleration.Serializer accelerationSerializer,
                                        final QuarkBuffer carEncodeBuffer,
                                        final QuarkBuffer tempBuffer1,
                                        final QuarkBuffer tempBuffer2) {
        return serializeTo(carSerializer.init(carEncodeBuffer),
                           engineSerializer,
                           performanceFigureSerializer,
                           fuelFigureSerializer,
                           accelerationSerializer,
                           tempBuffer1,
                           tempBuffer2);
    }

    final private int serializeTo(final QuarkBuffer buffer) {
        return serializeTo(carSerializer,
                           engineSerializer,
                           performanceFigureSerializer,
                           fuelFigureSerializer,
                           accelerationSerializer,
                           buffer,
                           tempBuffer1,
                           tempBuffer2);
    }

    final private int serializeTo(final Car car) {
        return serializeTo(car.serializer(1024),
                           engineSerializer,
                           performanceFigureSerializer,
                           fuelFigureSerializer,
                           accelerationSerializer,
                           tempBuffer1,
                           tempBuffer2);
    }

    final private int serializeTo(final Car.Serializer carSerializer) {
        return serializeTo(carSerializer,
                           engineSerializer,
                           performanceFigureSerializer,
                           fuelFigureSerializer,
                           accelerationSerializer,
                           tempBuffer1,
                           tempBuffer2);
    }

    final private void deserializeFrom(final QuarkBuffer buffer, final int len) {
        carDeserializer.init(buffer, 0, len).run(cb);
    }

    final private void deserializeFrom(final Car car) {
        car.deserializer().run(cb);
    }

    @Override
    public String name() {
        return "rumi.protobuf.serial";
    }

    @Override
    public Car create(final boolean encode) {
        final Car car = Car.create();
        final Car.Serializer serializer = car.serializer(1024);
        serializer.timestamp(UtlTime.nowSinceEpoch());
        if (encode) {
            carEncodedLength = serializeTo(serializer);
        }
        return car;
    }

    @Override
    public void prepareToEncode() {
    }

    @Override
    public void encode(final Car car) {
        carEncodedLength = serializeTo(car);
    }

    @Override
    public void encode() {
        carEncodedLength = serializeTo(carEncodeBuffer);
    }

    @Override
    public int encodedLength() {
        return carEncodedLength;
    }

    @Override
    public void postEncode() {
    }

    @Override
    public void prepareToDecode() {
    }

    @Override
    public void decode(final Car car) {
        deserializeFrom(car);
    }

    @Override
    public void decode() {
        deserializeFrom(carDecodeBuffer, carDecodeLength);
    }

    @Override
    public int decodedLength() {
        return carDecodeLength;
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
            cb.ts = 0l;
            decode(car);
            return cb.ts;
        }
        finally { 
            car.dispose();
        }
    }
}
