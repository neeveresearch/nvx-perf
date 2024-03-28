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
package com.neeve.perf.aep.engine;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.neeve.aep.AepEngine;
import com.neeve.aep.AepMessageSender;
import com.neeve.aep.annotations.EventHandler;
import com.neeve.perf.aep.engine.messages.FinalMessage;
import com.neeve.perf.aep.engine.messages.Latencies;
import com.neeve.perf.aep.engine.messages.LatencyType;
import com.neeve.perf.aep.engine.messages.Throughput;
import com.neeve.server.app.annotations.AppInjectionPoint;

class Processor {
    private File _bookFile;
    private XSSFWorkbook _book;
    private XSSFCell[] _cells;
    private boolean _outputThroughput;
    protected AepEngine _engine;
    protected AepMessageSender _messageSender;

    Processor() throws IOException {
        // output stats?
        final String outputFilename = System.getProperty(ConfigProperties.PROP_OUTPUT_FILE);
        if (outputFilename != null) {
            // open the workbook
            _bookFile = new File(outputFilename);
            if (!_bookFile.exists()) {
                throw new IllegalArgumentException("specified output file '" + outputFilename + "' does not exist");
            }
            if (!_bookFile.isFile()) {
                throw new IllegalArgumentException("specified output file '" + outputFilename + "' is a directory");
            }
            _book = new XSSFWorkbook(new FileInputStream(_bookFile));

            // open the first sheet
            XSSFSheet sheet;
            try {
                sheet = _book.getSheetAt(0);
                System.out.println("Output sheet is '" + sheet.getSheetName() + "'");
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("specified output file '" + outputFilename + "' does not have a sheet");
            }

            // parse whether to output throughput or latency stats
            _outputThroughput = Boolean.valueOf(System.getProperty(ConfigProperties.PROP_OUTPUT_THROUGHPUT));

            // parse output cell(s) in the opened sheet
            final String cellSpec = System.getProperty(ConfigProperties.PROP_OUTPUT_CELL);
            if (cellSpec == null) {
                throw new IllegalArgumentException("output cell needs to be specified");
            }
            final String[] splitCellSpec = cellSpec.split("-");
            if (splitCellSpec.length != 2) {
                throw new IllegalArgumentException("cell needs to be in <ROW>-<COLUMN> format");
            }
            int rowNum;
            try {
                rowNum = Integer.parseInt(splitCellSpec[0]);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("invalid row '" + splitCellSpec[0] + " in specified cell '" + cellSpec + "'");
            }
            final XSSFRow row = sheet.getRow(rowNum);
            if (row == null) {
                throw new IllegalArgumentException("row '" + splitCellSpec[0] + " is not in the sheet");
            }
            int cellNum;
            try {
                cellNum = Integer.parseInt(splitCellSpec[1]);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("invalid cell '" + splitCellSpec[1] + " in specified cell '" + cellSpec + "'");
            }
            _cells = new XSSFCell[3];
            _cells[0] = row.getCell(cellNum, Row.MissingCellPolicy.RETURN_NULL_AND_BLANK);
            if (_cells[0] == null) {
                throw new IllegalArgumentException("cell '" + cellNum + " is not in the sheet");
            }
            if (!_outputThroughput) {
                _cells[1] = row.getCell(++cellNum, Row.MissingCellPolicy.RETURN_NULL_AND_BLANK);
                if (_cells[1] == null) {
                    throw new IllegalArgumentException("cell '" + cellNum + " is not in the sheet");
                }
                _cells[2] = row.getCell(++cellNum, Row.MissingCellPolicy.RETURN_NULL_AND_BLANK);
                if (_cells[2] == null) {
                    throw new IllegalArgumentException("cell '" + cellNum + " is not in the sheet");
                }
            }
        }
    }

	@AppInjectionPoint
	final public void setEngine(AepEngine engine) {
		_engine = engine;
	}

	@AppInjectionPoint
	final public void setMessageSender(AepMessageSender messageSender) {
		_messageSender = messageSender;
	}

    @EventHandler
    final public void onMessage(final FinalMessage finalMessage) throws Exception {
        // set in output sheet
        if (_book != null) {
            // extract fields
            final int throughput = finalMessage.getThroughput().getPostWarmup();
            final double w2wMean = finalMessage.getLatencies().getW2w().getMean() / 1000;
            final double w2wMedian = finalMessage.getLatencies().getW2w().getPct50() / 1000.0;
            final double w2w99th = finalMessage.getLatencies().getW2w().getPct99() / 1000.0;

            // set in cell(s)
            if (_outputThroughput) {
                _cells[0].setCellValue(throughput);
            }
            else {
                _cells[0].setCellValue(w2wMean);
                _cells[1].setCellValue(w2wMedian);
                _cells[2].setCellValue(w2w99th);
            }

            // overwrite the workbook with the updated content
            final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(_bookFile));
            _book.write(bos);
            bos.close();

            // we're done with the book. close it
            _book.close();
        }

        // shut down the cluster
        _engine.setAsLastTransaction(null, true, true);
    }
}

