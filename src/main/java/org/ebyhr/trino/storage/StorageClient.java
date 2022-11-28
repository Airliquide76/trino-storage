/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ebyhr.trino.storage;

import io.airlift.log.Logger;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.spi.connector.ConnectorSession;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.ebyhr.trino.storage.operator.FilePlugin;
import org.ebyhr.trino.storage.operator.PluginFactory;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class StorageClient
{
    private static final Logger log = Logger.get(StorageClient.class);

    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public StorageClient(HdfsEnvironment hdfsEnvironment)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    public List<String> getSchemaNames()
    {
        return Stream.of(FileType.values())
            .map(FileType::toString)
            .collect(Collectors.toList());
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        return new HashSet<>();
    }

    public StorageTable getTable(ConnectorSession session, String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");

        FilePlugin plugin = PluginFactory.create(schema);
        try {
            List<StorageColumn> columns = plugin.getFields(tableName, path -> getInputStream(session, path));
            return new StorageTable(tableName, columns);
        }
        catch (Exception e) {
            log.error(e, "Failed to get table: %s.%s", schema, tableName);
            return null;
        }
    }

    public InputStream getInputStream(ConnectorSession session, String path)
    {
        try {
            if (path.startsWith("http://") || path.startsWith("https://")) {

                HttpGet request = new HttpGet(path.split("\\?")[0]);
                HttpClient client = HttpClientBuilder.create().build();
                if (path.contains("ui.boondmanager/api")){
                    URIBuilder uriBuilder = new URIBuilder(request.getURI());
                    String arguments = path.split("\\?")[1];
                    for (int i = 0; i < arguments.split("&").length; i++){
                        if (arguments.split("&")[i].split(Pattern.quote("=")).length==2){
                            uriBuilder = uriBuilder.addParameter(arguments.split("&")[i].split(Pattern.quote("="))[0].replace("date","Date"),arguments.split("&")[i].split(
                                Pattern.quote("="))[1]);
                        }else{
                            uriBuilder = uriBuilder.addParameter(arguments.split("&")[i].split(Pattern.quote("="))[0].replace("date","Date"),"");
                        }
                    }
                    URI uri = uriBuilder.build();
                    request.setURI(uri);
                    if (!session.getProperty("jwt", String.class).isEmpty()){
                        String jwt = session.getProperty("jwt",String.class);
                        request.setHeader("X-Jwt-Internal-Boondmanager", jwt);
                    }
                }
                HttpResponse response = client.execute(request);
                if (response.getStatusLine().getStatusCode() != 200){
                    throw new IOException(response.getStatusLine().getReasonPhrase());
                }

                return response.getEntity().getContent();
            }
            if (path.startsWith("hdfs://") || path.startsWith("s3a://") || path.startsWith("s3://")) {
                Path hdfsPath = new Path(path);
                return hdfsEnvironment.getFileSystem(new HdfsContext(session), hdfsPath).open(hdfsPath);
            }
            if (!path.startsWith("file:")) {
                path = "file:" + path;
            }

            return URI.create(path).toURL().openStream();
        }
        catch (IOException | URISyntaxException e) {
            throw new UncheckedIOException(format("Failed to open stream for %s", path), (IOException) e);
        }
    }
}
