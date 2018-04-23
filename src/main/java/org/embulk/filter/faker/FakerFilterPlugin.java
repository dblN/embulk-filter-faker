package org.embulk.filter.faker;

import com.google.common.base.Optional;

import com.github.javafaker.Faker;
import com.github.javafaker.Name;

import org.embulk.config.*;
import org.embulk.spi.*;
import org.slf4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.HashMap;
import java.util.Locale;

public class FakerFilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("columns")
        public List<ColumnConfig> getColumns();
    }

    interface ColumnConfig
            extends Task
    {
        @Config("name")
        public String getName();

        @Config("faker_expression")
        public String getFakerExpression();

        @Config("locale")
        public String getLocale();
    }

    private static final Logger log = Exec.getLogger(FakerFilterPlugin.class);

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema outputSchema = inputSchema;

        for (ColumnConfig column : task.getColumns()) {
            inputSchema.lookupColumn(column.getName());
        }

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema,
            Schema outputSchema, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        final int[] targetColumns = new int[task.getColumns().size()];
        int i = 0;
        for (ColumnConfig column : task.getColumns()) {
            targetColumns[i++] = inputSchema.lookupColumn(column.getName()).getIndex();
        }

        final HashMap<String, ColumnConfig> configHashMap = new HashMap<>(task.getColumns().size());
        for (ColumnConfig column : task.getColumns()) {
            configHashMap.put(column.getName(), column);
        }

        final HashMap<String, Faker> fakers = new HashMap<String, Faker>();

        return new PageOutput() {
            private final PageReader pageReader = new PageReader(inputSchema);
            private final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

            @Override
            public void finish()
            {
                pageBuilder.finish();
            }

            @Override
            public void close()
            {
                pageBuilder.close();
            }

            private boolean isTargetColumn(Column c)
            {
                for (int i = 0; i < targetColumns.length; i++) {
                    if (c.getIndex() == targetColumns[i]) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);

                while (pageReader.nextRecord()) {
                    inputSchema.visitColumns(new ColumnVisitor() {
                        @Override
                        public void booleanColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            }
                            else {
                                pageBuilder.setBoolean(column, pageReader.getBoolean(column));
                            }
                        }

                        @Override
                        public void longColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            }
                            else {
                                pageBuilder.setLong(column, pageReader.getLong(column));
                            }
                        }

                        @Override
                        public void doubleColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            }
                            else {
                                pageBuilder.setDouble(column, pageReader.getDouble(column));
                            }
                        }

                        @Override
                        public void stringColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            }
                            else if (isTargetColumn(column)) {
                                ColumnConfig config = configHashMap.get(column.getName());

                                Faker faker;
                                if (fakers.containsKey(config.getLocale())) {
                                    faker = fakers.get(config.getLocale());
                                } else {
                                    faker = new Faker(new Locale(config.getLocale()));
                                    fakers.put(config.getLocale(), faker);
                                }
                                pageBuilder.setString(column, faker.expression(config.getFakerExpression()));
                            }
                            else {
                                pageBuilder.setString(column, pageReader.getString(column));
                            }
                        }

                        @Override
                        public void timestampColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            }
                            else {
                                pageBuilder.setTimestamp(column, pageReader.getTimestamp(column));
                            }
                        }

                        @Override
                        public void jsonColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            }
                            else {
                                pageBuilder.setJson(column, pageReader.getJson(column));
                            }
                        }
                    });
                    pageBuilder.addRecord();
                }
            }
        };
    }
}
