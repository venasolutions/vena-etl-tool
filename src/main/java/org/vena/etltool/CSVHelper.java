package org.vena.etltool;

import org.apache.commons.csv.CSVFormat;
import org.vena.etltool.entities.ETLFileImportStepDTO.FileFormat;

public class CSVHelper {

	public static CSVFormat getCSVFormat(FileFormat fileFormat) {
        if (fileFormat == null)
        	fileFormat = FileFormat.CSV;

        switch (fileFormat) {
            case CSV:
                return CSVFormat.RFC4180;
            case PSV:
                return CSVFormat.Builder.create().setDelimiter('|').setRecordSeparator("\r\n").build();
            case TDF:
                return CSVFormat.TDF;
            default:
                throw new IllegalArgumentException("Unsupported file format " + fileFormat);
        }
    }

}
