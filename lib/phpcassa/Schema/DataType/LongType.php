<?php
namespace phpcassa\Schema\DataType;

/**
 * Stores data as an 8-byte signed integer.
 *
 * @package phpcassa\Schema\DataType
 */
class LongType extends CassandraType {

    public function pack($value, $is_name=null, $slice_end=null, $is_data=null)
    {
        // If we are on a 32bit architecture we have to explicitly deal with
        // 64-bit twos-complement arithmetic since PHP wants to treat all ints
        // as signed and any int over 2^31 - 1 as a float
        if (PHP_INT_SIZE == 4) {
            $neg = $value < 0;

            if ($neg) {
              $value *= -1;
            }

            $hi = (int)($value / 4294967296);
            $lo = (int)$value;

            if ($neg) {
                $hi = ~$hi;
                $lo = ~$lo;
                if (($lo & (int)0xffffffff) == (int)0xffffffff) {
                    $lo = 0;
                    $hi++;
                } else {
                    $lo++;
                }
            }
            $data = pack('N2', $hi, $lo);
        } else {
            $hi = $value >> 32;
            $lo = $value & 0xFFFFFFFF;
            $data = pack('N2', $hi, $lo);
        }
        return $data;
    }

    public function unpack($data, $is_name=null)
    {
        $arr = unpack('N2', $data);

        // If we are on a 32bit architecture we have to explicitly deal with
        // 64-bit twos-complement arithmetic since PHP wants to treat all ints
        // as signed and any int over 2^31 - 1 as a float
        if (PHP_INT_SIZE == 4) {

            $hi = $arr[1];
            $lo = $arr[2];
            $isNeg = $hi  < 0;

            // Check for a negative
            if ($isNeg) {
                $hi = ~$hi & (int)0xffffffff;
                $lo = ~$lo & (int)0xffffffff;

                if ($lo == (int)0xffffffff) {
                    $hi++;
                    $lo = 0;
                } else {
                    $lo++;
                }
            }

            // Force 32bit words in excess of 2G to pe positive - we deal wigh sign
            // explicitly below

            if ($hi & (int)0x80000000) {
                $hi &= (int)0x7fffffff;
                $hi += 0x80000000;
            }

            if ($lo & (int)0x80000000) {
                $lo &= (int)0x7fffffff;
                $lo += 0x80000000;
            }

            $value = $hi * 4294967296 + $lo;

            if ($isNeg)
                $value = 0 - $value;

        } else {
            // Upcast negatives in LSB bit
            if ($arr[2] & 0x80000000)
                $arr[2] = $arr[2] & 0xffffffff;

            // Check for a negative
            if ($arr[1] & 0x80000000) {
                $arr[1] = $arr[1] & 0xffffffff;
                $arr[1] = $arr[1] ^ 0xffffffff;
                $arr[2] = $arr[2] ^ 0xffffffff;
                $value = 0 - $arr[1]*4294967296 - $arr[2] - 1;
            } else {
                $value = $arr[1]*4294967296 + $arr[2];
            }
        }
        return $value;
    }
}

