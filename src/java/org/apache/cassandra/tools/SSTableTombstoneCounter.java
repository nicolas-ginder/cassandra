/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.codehaus.jackson.JsonGenerator;

/**
 * Created by nico on 9/22/16.
 */
public class SSTableTombstoneCounter
{


    private static CommandLine cmd;


    public static void main(String[] args) throws ConfigurationException, IOException
    {

        String usage = String.format("Usage: %s [-l] <sstable> [<sstable> ...]%n", SSTableTombstoneCounter.class.getName());

        final Options options = new Options();

        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e1)
        {
            System.err.println(e1.getMessage());
            System.err.println(usage);
            System.exit(1);
        }


        if (cmd.getArgs().length != 1)
        {
            System.err.println("You must supply exactly one sstable");
            System.err.println(usage);
            System.exit(1);
        }



        String ssTableFileName = new File(cmd.getArgs()[0]).getAbsolutePath();

        DatabaseDescriptor.loadSchemas(false);
        Descriptor descriptor = Descriptor.fromFilename(ssTableFileName);

        // Start by validating keyspace name
        if (Schema.instance.getKSMetaData(descriptor.ksname) == null)
        {
            System.err.println(String.format("Filename %s references to nonexistent keyspace: %s!",
                                             ssTableFileName, descriptor.ksname));
            System.exit(1);
        }
        Keyspace keyspace = Keyspace.open(descriptor.ksname);
        PrintStream out = System.out;

        String baseName = descriptor.cfname;
        ColumnFamilyStore cfStore = null;
        try {
            cfStore = keyspace.getColumnFamilyStore(baseName);
        } catch (IllegalArgumentException e) {
            System.err.println(String.format("The provided column family is not part of this cassandra keyspace: keyspace = %s, column family = %s",
                                             descriptor.ksname, descriptor.cfname));
            System.exit(1);
        }
        SSTableReader reader = SSTableReader.open(descriptor, cfStore.metadata);
        ISSTableScanner scanner = reader.getScanner();

        long totalTombstones = 0, totalColumns = 0;

            out.printf(descriptor.baseFilename() + "\n");
            out.printf("rowkey #tombstones (#columns)\n");

        while (scanner.hasNext()) {
            SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();

            int tombstonesCount = 0, columnsCount = 0;
            while (row.hasNext()) {
                OnDiskAtom column = row.next();
                if (column instanceof RangeTombstone) {
                    tombstonesCount++;
                }

                //out.println("******"+column.name());
                columnsCount++;
            }
            totalTombstones += tombstonesCount;
            totalColumns += columnsCount;

            if (tombstonesCount > 0) {
                String key = row.getColumnFamily().metadata().getKeyValidator().getString(row.getKey().getKey());
                out.printf("%s #%d (#%d)%n", key, tombstonesCount, columnsCount);
            }

        }


        out.printf("#total_tombstones (#total_columns)\n");

        out.printf("#%d (#%d)%n", totalTombstones, totalColumns);

        scanner.close();




        System.exit(0);
    }




    public static void main2(String[] args) throws IOException, ParseException
    {
        String usage = String.format("Usage: %s [-l] <sstable> [<sstable> ...]%n", SSTableTombstoneCounter.class.getName());

        final Options options = new Options();
        options.addOption("l", "legend", false, "Include column name explanation");
        options.addOption("p", "partitioner", true, "The partitioner used by database");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.getArgs().length < 1) {
            System.err.println("You must supply at least one sstable");
            System.err.println(usage);
            System.exit(1);
        }

        // Fake DatabaseDescriptor settings so we don't have to load cassandra.yaml etc
        Config.setClientMode(true);
        String partitionerName = String.format("org.apache.cassandra.dht.%s",
                                               cmd.hasOption("p") ? cmd.getOptionValue("p") : "RandomPartitioner");
        try {
            Class<?> clazz = Class.forName(partitionerName);
            IPartitioner partitioner = (IPartitioner) clazz.newInstance();
            DatabaseDescriptor.setPartitioner(partitioner);
        } catch (Exception e) {
            throw new RuntimeException("Can't instantiate partitioner " + partitionerName);
        }

        PrintStream out = System.out;

        for (String arg : cmd.getArgs()) {
            String ssTableFileName = new File(arg).getAbsolutePath();
            DatabaseDescriptor.loadSchemas(false);
            Descriptor descriptor = Descriptor.fromFilename(ssTableFileName);

            run(descriptor, cmd, out);
        }

        System.exit(0);
    }

    private static void run(Descriptor desc, CommandLine cmd, PrintStream out) throws IOException {


        if (Schema.instance.getKSMetaData(desc.ksname) == null)
        {
            System.err.println(String.format(" references to nonexistent keyspace: %s!",
                                             desc.ksname));
            System.exit(1);
        }

        Keyspace keyspace = Keyspace.open(desc.ksname);

        String baseName = desc.cfname;
        if (desc.cfname.contains("."))
        {
            String[] parts = desc.cfname.split("\\.", 2);
            baseName = parts[0];
        }
        // IllegalArgumentException will be thrown here if ks/cf pair does not exist
        ColumnFamilyStore cfStore = null;
        try {
            cfStore = keyspace.getColumnFamilyStore(baseName);
        } catch (IllegalArgumentException e) {
            System.err.println(String.format("The provided column family is not part of this cassandra keyspace: keyspace = %s, column family = %s",
                                             desc.ksname, desc.cfname));
            System.exit(1);
        }
        SSTableReader reader = SSTableReader.open(desc, cfStore.metadata);
        ISSTableScanner scanner = reader.getScanner();

        long totalTombstones = 0, totalColumns = 0;
        if (cmd.hasOption("l")) {
            out.printf(desc.baseFilename() + "\n");
            out.printf("rowkey #tombstones (#columns)\n");
        }
        while (scanner.hasNext()) {
            SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();

            int tombstonesCount = 0, columnsCount = 0;
            while (row.hasNext()) {
                OnDiskAtom column = row.next();
                if (column instanceof ColumnFamily && ((ColumnFamily) column).isMarkedForDelete()) {
                    tombstonesCount++;
                }
                columnsCount++;
            }
            totalTombstones += tombstonesCount;
            totalColumns += columnsCount;

            if (tombstonesCount > 0) {
                String key;
                try {
                    key = UTF8Type.instance.getString(row.getKey().getKey());
                } catch (RuntimeException e) {
                    key = BytesType.instance.getString(row.getKey().getKey());
                }
                out.printf("%s %d (%d)%n", key, tombstonesCount, columnsCount);
            }

        }

        if (cmd.hasOption("l")) {
            out.printf("#total_tombstones (#total_columns)\n");
        }
        out.printf("%d (%d)%n", totalTombstones, totalColumns);

        scanner.close();
    }


}
