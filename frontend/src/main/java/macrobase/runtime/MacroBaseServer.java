package macrobase.runtime;

import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.visualization.datasource.datatable.DataTable;
import edu.stanford.futuredata.macrobase.serializer.DataTableSerializer;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.ingest.SQLIngester;
import macrobase.runtime.command.MacroBasePipelineCommand;
import macrobase.runtime.resources.*;
import org.eclipse.jetty.server.session.SessionHandler;
import org.glassfish.jersey.logging.LoggingFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.logging.Level;

public class MacroBaseServer extends Application<MacroBaseConf> {
    private static final Logger log = LoggerFactory.getLogger(MacroBaseServer.class);

    public static void main(String[] args) throws Exception {
        new MacroBaseServer().run(args);
    }

    @Override
    public String getName() {
        return "macrobase";
    }

    @Override
    public void initialize(Bootstrap<MacroBaseConf> bootstrap) {
        SimpleModule module = new SimpleModule();
        module.addSerializer(DataTable.class, new DataTableSerializer());
        bootstrap.getObjectMapper().registerModule(module);
        bootstrap.addBundle(new AssetsBundle("/login", "/", "index.html"));
    }

    @Override
    public void run(MacroBaseConf configuration,
                    Environment environment) throws Exception {
        configuration.loadSystemProperties();
      environment.servlets().setSessionHandler(new
          SessionHandler());
        environment.jersey().register(new AnalyzeResource(configuration));
        environment.jersey().register(new SchemaResource(configuration));
        environment.jersey().register(new RowSetResource(configuration));
        environment.jersey().register(new FormattedRowSetResource(configuration));
        environment.jersey().register(new MultipleRowSetResource(configuration));
        environment.jersey().register(new LoginResource(configuration));
        environment.jersey().register(new RefreshTablesResource(configuration));
        environment.jersey().register(new SampleRowsResource(configuration));
        environment.jersey().register(new FastInsightResource(configuration));
        environment.jersey().register(new DeepInsightResource(configuration));
        environment.jersey().register(new GraphResource(configuration));
        environment.healthChecks().register("basic", new HealthCheck() {
            @Override
            protected Result check() throws Exception {
                return null;
            }
        });
        environment.jersey().setUrlPattern("/api/*");
        environment.jersey().register(new LoggingFeature(java.util.logging.Logger.getLogger(LoggingFeature.DEFAULT_LOGGER_NAME), Level.SEVERE, LoggingFeature.Verbosity.PAYLOAD_ANY,
                1000));
        MacroBaseConf confNew = configuration.copy();
        confNew.set(MacroBaseConf.DB_USER, "app");
        confNew.set(MacroBaseConf.DB_PASSWORD, "app");
        SQLIngester ingester = (SQLIngester) BaseResource.getLoader(confNew);
        String ddl = "create table if not exists " + MacroBaseDefaults.BOCA_CORRELATION_TABLE +
                " (tableName String, kpiColumn String, pearson String, chiSquare String, annova String) using row";
        ingester.executeSQL(ddl);
    }
}
