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

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryStream;
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
	public boolean isLocked(final String pid, final String fileName)  throws IOException {
		final Path path = Paths.get("/proc/" + pid);
		if (Files.exists(path)) {
			boolean result = false; 
			try (DirectoryStream<Path> fdStream = Files.newDirectoryStream(Paths.get("/proc/" + pid + "/fd"))) {
				for (Path fd : fdStream) {
					if (Files.isSymbolicLink(fd)) {
						// Only symlinks here!!!
						if (Files.readSymbolicLink(fd).startsWith(fileName)) {
							result = true;
							break;
						}
					}
				}
			} catch (Exception ex) {
				if (ex instanceof AccessDeniedException)
					result = false;
				else
					throw new IOException("Something wrong with /proc filesystem!\n", ex);
			}
			return result;
		} else {
			return false;
		}
	}

}
