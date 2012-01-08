package org.countandra.exceptions;

public class CountandraException extends Exception {
	private Reason reason;

	public enum Reason {
		TIMEDIMENSIONNOTSUPPORTED(1, "Time Dimension is not supported"), MALFORMEDQUERY(
				2, "The Query is malformed"), MISMATCHEDQUERY(3,
				"The Query is mismatched"), UNKNOWNERROR(4, "Unknown Error");

		private int code;
		private String desc;

		private Reason(int code, String desc) {
			this.code = code;
			this.desc = desc;
		}

		public int getCode() {
			return code;
		}

		public String getDesc() {
			return desc;
		}
	}

	public CountandraException(Reason reason) {
		this.reason = reason;
	}

}
