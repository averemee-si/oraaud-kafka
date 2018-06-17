/**
 * Copyright (c) 2018-present, http://a2-solutions.eu
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package eu.solutions.a2.audit.utils.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author averemee-si
 * 
 * Works with /proc filesystem (Linux, Solaris, AIX)
 *
 */
public class OpenFileSystemV implements OpenFilesIntf {

	@Override
	public boolean isLocked(String fileName)  throws IOException {
		String pid = fileName.substring(fileName.lastIndexOf(File.separator) + 1, fileName.lastIndexOf('_'));
		pid = pid.substring(pid.lastIndexOf('_') + 1);
		Path path = Paths.get("/proc/" + pid);
		if (Files.exists(path)) {
			return true;
		} else {
			return false;
		}
	}

}
