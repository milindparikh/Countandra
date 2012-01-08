/*Copyright 2012  Countandra

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
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
