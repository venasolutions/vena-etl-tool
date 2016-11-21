package org.vena.etltool.entities;

public abstract class ETLFileImportStepDTO extends ETLStepDTO {

	public enum FileFormat {CSV, TDF, PSV}

	private DataType dataType;

	private String mimePart;

	private FileFormat fileFormat;

	private String fileName;

	private String serverFileName;

	private String s3FileName;

	private int linesProcessed;

	private int resumeLine;

	private long bytesProcessed;

	private long bytesTotal;

	private boolean fileUploadSuccessful = false;

	private int skipInvalidLines;

	private int numInvalidLines;

	private boolean compressed = true;

	public ETLFileImportStepDTO() {
	}

	public ETLFileImportStepDTO(ETLFileOldDTO etlFile) {
		this.dataType = etlFile.getFileType();
		this.mimePart = etlFile.getMimePart();
		this.fileFormat = etlFile.getFileFormat();
		this.fileName = etlFile.getFilename();
		this.serverFileName = etlFile.getServerFilename();
		this.linesProcessed = etlFile.getLinesProcessed();
		this.bytesProcessed = 0;
		if (etlFile.getDone()) {
			this.status = Status.COMPLETED;
			this.percentDone = 100;
		}
	}

	public DataType getDataType() {
		return dataType;
	}

	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}

	public String getMimePart() {
		return mimePart;
	}

	public void setMimePart(String mimePart) {
		this.mimePart = mimePart;
	}

	public FileFormat getFileFormat() {
		return fileFormat;
	}

	public void setFileFormat(FileFormat fileFormat) {
		this.fileFormat = fileFormat;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getServerFileName() {
		return serverFileName;
	}

	public void setServerFileName(String serverFileName) {
		this.serverFileName = serverFileName;
	}

	public String getS3FileName() {
		return s3FileName;
	}

	public void setS3FileName(String s3FileName) {
		this.s3FileName = s3FileName;
	}

	public int getLinesProcessed() {
		return linesProcessed;
	}

	public void setLinesProcessed(int linesProcessed) {
		this.linesProcessed = linesProcessed;
	}

	public int getResumeLine() {
		return resumeLine;
	}

	public void setResumeLine(int line) {
		this.resumeLine = line;
	}

	public long getBytesProcessed() {
		return bytesProcessed;
	}

	public void setBytesProcessed(long bytesProcessed) {
		this.bytesProcessed = bytesProcessed;
	}

	public long getBytesTotal() {
		return bytesTotal;
	}

	public void setBytesTotal(long bytesTotal) {
		this.bytesTotal = bytesTotal;
	}
	
	public boolean getFileUploadSuccessful() {
		return this.fileUploadSuccessful;
	}
	
	public void setFileUploadSuccessful(boolean fileUploadSuccessful) {
		this.fileUploadSuccessful = fileUploadSuccessful;
	}

	public int getSkipInvalidLines() {
		return skipInvalidLines;
	}

	public void setSkipInvalidLines(int maxLineErrors) {
		this.skipInvalidLines = maxLineErrors;
	}

	public int getNumInvalidLines() {
		return numInvalidLines;
	}

	public void setNumInvalidLines(int numErrors) {
		this.numInvalidLines = numErrors;
	}

	public boolean isCompressed() { return compressed; }

	public void setCompressed(boolean compressed) { this.compressed = compressed; }
}
