<?php
require_once(__DIR__.'/../AutopackBase.php');

use phpcassa\ColumnFamily;
use phpcassa\ColumnSlice;

abstract class StandardBase extends AutopackBase {

    protected function make_group($cf, $cols) {
        if ($this->SERIALIZED) {
            $dict = array();
            $serialized_cols = array();
            for ($i = 0; $i < count($cols); $i++) {
                $name = $cols[$i];
                $name = serialize($name);
                $dict[$name] = self::$VALS[$i];
                $serialized_cols[] = $name;
            }
        } else {
            $dict = array($cols[0] => self::$VALS[0],
                          $cols[1] => self::$VALS[1],
                          $cols[2] => self::$VALS[2]);
            $serialized_cols = $cols;
        }

        return array(
            'cf' => $cf,
            'cols' => $cols,
            'serialized_cols' => $serialized_cols,
            'dict' => $dict
        );
    }

    public function test_standard_column_family() {
        $type_groups = $this->make_type_groups();

        foreach($type_groups as $group) {
            $cf = $group['cf'];
            $dict = $group['dict'];
            $cols = $group['cols'];
            $serialized_cols = $group['serialized_cols'];

            $cf->insert(self::$KEYS[0], $dict);
            $this->assertEquals($dict, $cf->get(self::$KEYS[0]));

            # Check each column individually
            foreach(range(0,2) as $i)
                $this->assertEquals(array($serialized_cols[$i] => self::$VALS[$i]),
                    $cf->get(self::$KEYS[0], null, array($cols[$i])));

            # Check with list of all columns
            $this->assertEquals($dict, $cf->get(self::$KEYS[0], null, $cols));

            # Same thing but with start and end
            $column_slice = new ColumnSlice($cols[0], $cols[2]);
            $this->assertEquals($dict, $cf->get(self::$KEYS[0], $column_slice));

            # Start and end are the same
            $column_slice = new ColumnSlice($cols[0], $cols[0]);
            $this->assertEquals(array($serialized_cols[0] => self::$VALS[0]),
                $cf->get(self::$KEYS[0], $column_slice));



            ### remove() tests ###

            $cf->remove(self::$KEYS[0], array($cols[0]));
            $this->assertEquals(2, $cf->get_count(self::$KEYS[0]));

            $cf->remove(self::$KEYS[0], array($cols[1], $cols[2]));
            $this->assertEquals(0, $cf->get_count(self::$KEYS[0]));

            # Insert more than one row
            $cf->insert(self::$KEYS[0], $dict);
            $cf->insert(self::$KEYS[1], $dict);
            $cf->insert(self::$KEYS[2], $dict);


            ### multiget() tests ###

            $result = $cf->multiget(self::$KEYS);
            foreach(range(0,2) as $i)
                $this->assertEquals($dict, $result[self::$KEYS[0]]);

            $result = $cf->multiget(array(self::$KEYS[2]));
            $this->assertEquals($dict, $result[self::$KEYS[2]]);

            # Check each column individually
            foreach(range(0,2) as $i) {
                $result = $cf->multiget(self::$KEYS, null, array($cols[$i]));
                foreach(range(0,2) as $j)
                    $this->assertEquals(array($serialized_cols[$i] => self::$VALS[$i]),
                                        $result[self::$KEYS[$j]]);
            }

            # Check that if we list all columns, we get the full dict
            $result = $cf->multiget(self::$KEYS, null, $cols);
            foreach(range(0,2) as $i)
                $this->assertEquals($dict, $result[self::$KEYS[$j]]);

            # The same thing with a start and end instead
            $column_slice = new ColumnSlice($cols[0], $cols[2]);
            $result = $cf->multiget(self::$KEYS, $column_slice);
            foreach(range(0,2) as $i)
                $this->assertEquals($dict, $result[self::$KEYS[$j]]);

            # A start and end that are the same
            $column_slice = new ColumnSlice($cols[0], $cols[0]);
            $result = $cf->multiget(self::$KEYS, $column_slice);
            foreach(range(0,2) as $i)
                $this->assertEquals(array($serialized_cols[0] => self::$VALS[0]),
                                    $result[self::$KEYS[$j]]);


            ### get_range() tests ###

            $result = $cf->get_range($key_start=self::$KEYS[0]);
            foreach($result as $subres)
                $this->assertEquals($dict, $subres);

            $column_slice = new ColumnSlice($cols[0], $cols[2]);
            $result = $cf->get_range($key_start=self::$KEYS[0], $key_finish='',
                                     $key_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                     $column_slice);
            foreach($result as $subres)
                $this->assertEquals($dict, $subres);

            $result = $cf->get_range($key_start=self::$KEYS[0], $key_finish='',
                                     $key_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                     $column_slice=null, $column_names=$cols);
            foreach($result as $subres)
                $this->assertEquals($dict, $subres);
        }
    }
}
