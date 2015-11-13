﻿// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Text;
using System.IO;

namespace Microsoft.Spark.CSharp.Interop.Ipc
{
    /// <summary>
    /// Serialization and Deserialization of data types between JVM & CLR
    /// </summary>
    public class SerDe //TODO - add ToBytes() for other types
    {
        public static byte[] ToBytes(bool value)
        {
            return new[] { System.Convert.ToByte(value) };
        }

        public static byte[] ToBytes(string value)
        {
            return Encoding.UTF8.GetBytes(value);
        }

        public static byte[] ToBytes(int value)
        {
            var byteRepresentationofInputLength = BitConverter.GetBytes(value);
            Array.Reverse(byteRepresentationofInputLength);
            return byteRepresentationofInputLength;
        }
        public static byte[] ToBytes(long value)
        {
            var byteRepresentationofInputLength = BitConverter.GetBytes(value);
            Array.Reverse(byteRepresentationofInputLength);
            return byteRepresentationofInputLength;
        }

        public static byte[] ToBytes(double value)
        {
            var byteRepresentationofInputLength = BitConverter.GetBytes(value);
            Array.Reverse(byteRepresentationofInputLength);
            return byteRepresentationofInputLength;
        }

        public static char ToChar(byte value)
        {
            return System.Convert.ToChar(value);
        }

        public static string ToString(byte[] value)
        {
            return Encoding.UTF8.GetString(value);
        }

        public static int ToInt(byte[] value)
        {
            return BitConverter.ToInt32(value, 0);
        }

        public static int Convert(int value)
        {
            var buffer = BitConverter.GetBytes(value);
            Array.Reverse(buffer); //Netty byte order is BigEndian
            return BitConverter.ToInt32(buffer, 0);
        }

        public static long Convert(long value)
        {
            var buffer = BitConverter.GetBytes(value);
            Array.Reverse(buffer); //Netty byte order is BigEndian
            return BitConverter.ToInt64(buffer, 0);
        }

        public static double Convert(double value)
        {
            var buffer = BitConverter.GetBytes(value);
            Array.Reverse(buffer); //Netty byte order is BigEndian
            return BitConverter.ToDouble(buffer, 0);
        }

        public static int ReadInt(Stream s)
        {
            byte[] buffer = ReadBytes(s, 4);
            return //Netty byte order is BigEndian
                (int)buffer[3] | 
                (int)buffer[2] << 8 | 
                (int)buffer[1] << 16 | 
                (int)buffer[0] << 24;
        }
        
        public static long ReadLong(Stream s)
        {
            byte[] buffer = ReadBytes(s, 8);
            return //Netty byte order is BigEndian
                (long)buffer[7] |
                (long)buffer[6] << 8 |
                (long)buffer[5] << 16 |
                (long)buffer[4] << 24 |
                (long)buffer[3] << 32 |
                (long)buffer[2] << 40 |
                (long)buffer[1] << 48 |
                (long)buffer[0] << 56;
        }
        
        public static double ReadDouble(Stream s)
        {
            byte[] buffer = ReadBytes(s, 8);
            Array.Reverse(buffer); //Netty byte order is BigEndian
            return BitConverter.ToDouble(buffer, 0);
        }
        
        public static string ReadString(Stream s)
        {
            return ToString(ReadBytes(s));
        }
        
        public static byte[] ReadBytes(Stream s, int length)
        {
            if (length <= 0)
                return null;
            byte[] buffer = new byte[length];
            int bytesRead = 0;
            while (bytesRead < length)
            {
                bytesRead += s.Read(buffer, bytesRead, length - bytesRead);
            }
            return buffer;
        }
        
        public static byte[] ReadBytes(Stream s)
        {
            var length = ReadInt(s);
            return ReadBytes(s, length);
        }
        
        public static string ReadObjectId(Stream s)
        {
            var type = s.ReadByte();

            if (type == 'n')
                return null;
            
            if (type != 'j')
            {
                Console.WriteLine("Expecting java object identifier type");
                return null;
            }

            return ReadString(s);
        }
        
        public static void Write(Stream s, byte value)
        {
            s.WriteByte(value);
        }

        public static void Write(Stream s, byte[] value)
        {
            s.Write(value, 0, value.Length);
        }

        public static void Write(Stream s, int value)
        {
            Write(s, new byte[] { 
                (byte)(value >> 24),
                (byte)(value >> 16), 
                (byte)(value >> 8), 
                (byte)value
            });
        }

        public static void Write(Stream s, long value)
        {
            Write(s, new byte[] { 
                (byte)(value >> 56),
                (byte)(value >> 48), 
                (byte)(value >> 40), 
                (byte)(value >> 32), 
                (byte)(value >> 24), 
                (byte)(value >> 16), 
                (byte)(value >> 8), 
                (byte)value,
            });
        }

        public static void Write(Stream s, double value)
        {
            byte[] buffer = BitConverter.GetBytes(value);
            Array.Reverse(buffer);
            Write(s, buffer);
        }

        public static void Write(Stream s, string value)
        {
            byte[] buffer = Encoding.UTF8.GetBytes(value);
            Write(s, buffer.Length);
            Write(s, buffer);
        }
    }
}
