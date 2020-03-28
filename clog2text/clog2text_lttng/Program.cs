/*++

    Copyright (c) Microsoft Corporation.
    Licensed under the MIT License.

Abstract:

    Reads from LTTNG (via babletrace output) and converts into CLOG property bag.  This bag is then sent into a generic CLOG function for output to STDOUT

    LTTTNG -> babletrace -> clog2text_lttng -> generic clog

--*/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using clogutils;
using clogutils.ConfigFile;
using CommandLine;
using static clogutils.CLogConsoleTrace;

namespace clog2text_lttng
{
    internal class Program
    {
        private static Dictionary<string, IClogEventArg> SplitBabelTraceLine(string traceLine)
        {
            int bracketCount = 0;
            Dictionary<string, IClogEventArg> ret = new Dictionary<string, IClogEventArg>();
            string piece = "";
            int lastEqual = -1;
            int startIndex = 0;

            for (int i = 0;
                    i < traceLine.Length + 1;
                    ++i) //<-- go one beyond the array, we catch this in the if block below
            {
                if ((i >= traceLine.Length || traceLine[i] == ',') && 0 == bracketCount)
                {
                    string key = traceLine.Substring(startIndex, lastEqual - startIndex).Trim();
                    string value = traceLine.Substring(lastEqual + 1, i - lastEqual - 1).Trim();
                    ret[key] = new LTTNGClogEvent(value);
                    piece = "";
                    startIndex = i + 1;
                    lastEqual = -1;
                    continue;
                }

                if (traceLine[i] == '{' || i >= 1 && traceLine[i] == '"' && traceLine[i - 1] == '/')
                {
                    ++bracketCount;
                }
                else if (bracketCount >= 1 && (traceLine[i] == '}' || i >= 1 && traceLine[i] == '"' && traceLine[i - 1] == '/'))
                {
                    --bracketCount;
                }
                else if (traceLine[i] == '=' && 0 == bracketCount)
                {
                    lastEqual = i;
                }

                piece += traceLine[i];
            }

            return ret;
        }

        private class ParallelPrinter
        {
            private TextReader _reader;

            public delegate string NextDelegate(string a);
            public delegate void PrintDelegate(string msg);
            public delegate string DoWorkDelegate(string input);

            private NextDelegate nextPfn;
            private PrintDelegate printPfn;
            private DoWorkDelegate _workPfn;

            private int _highMark = 0;
            private int _maxThreads = 20;
            private SortedList<int, string> _finishedItems = new SortedList<int, string>();
            private SortedList<int, string> _waitingItems = new SortedList<int, string>();

            private List<Thread> _outstandingThreads = new List<Thread>();
            private Semaphore _queuedInputStringsSemaphore = new Semaphore(0, 10000);
            private Semaphore _processingStringsSemaphore = new Semaphore(10000, 10000);
            private ManualResetEvent _finishedProducingEvent = new ManualResetEvent(false);
            private ManualResetEvent _finishedConsumingEvent = new ManualResetEvent(false);

            public ParallelPrinter(NextDelegate d, PrintDelegate p, DoWorkDelegate dw) 
            {
                nextPfn = d;
                printPfn = p;
                _workPfn = dw;

                int count = 0;
                string line;
                            
                while(null != (line = nextPfn(null)))
                {
                    bool needThread = false;
                    
                    _processingStringsSemaphore.WaitOne();

                    lock(_waitingItems)
                    {
                        //Console.WriteLine("Queue : " + count);
                        _waitingItems[count] = line;
                        if(_waitingItems.Count > 1000 && _outstandingThreads.Count < _maxThreads || 0 == _outstandingThreads.Count)
                        {
                            Thread t = new Thread(ProcessingThread);
                            t.Start();
                            _outstandingThreads.Add(t);
                        }                        
                    }
                    _queuedInputStringsSemaphore.Release();
                    ++count;
                }

                _finishedProducingEvent.Set();
                _finishedConsumingEvent.WaitOne();
            }


            private bool _someoneWriting = false;

            private void ProcessingThread()
            {
                for(; ; )
                {                    
                    WaitHandle[] waiters = new WaitHandle[2];
                    waiters[0] = _queuedInputStringsSemaphore;
                    waiters[1] = _finishedProducingEvent;

                    if (1 == WaitHandle.WaitAny(waiters))
                    {
                        lock (_waitingItems)
                        {
                            if (0 == _waitingItems.Count)
                                break;
                        }
                    }

                    KeyValuePair<int, string> item;
                    lock(_waitingItems)
                    {
                        item = _waitingItems.First();
                        _waitingItems.Remove(item.Key);
                    }

                    string result = _workPfn(item.Value);
                    bool haveWritingDuties = false;

                    lock (_finishedItems)
                    {
                        _finishedItems.Add(item.Key, result);

                        if (!_someoneWriting && _finishedItems.Count > 0 && _highMark == _finishedItems.First().Key)
                        {
                            haveWritingDuties = true;
                            _someoneWriting = true;
                        }
                    }
                    
                    if (haveWritingDuties)
                    {
                        bool haveWork = true;
                        while(haveWork)
                        {
                            string toPrint;
                            List<string> toPrintList = new List<string>();

                            lock (_finishedItems)
                            {
                                for(; ; )
                                {
                                    haveWork = false;
                                    if (_finishedItems.Count == 0)
                                        break;

                                    KeyValuePair<int, string> first = _finishedItems.First();
                                    if (_highMark != first.Key)
                                        break;
                                    
                                    haveWork = true;
                                    toPrintList.Add(first.Value);
                                    _finishedItems.Remove(first.Key);
                                    ++_highMark;
                                }
                            }

                            foreach (string s in toPrintList)
                            {
                                printPfn(s);
                                _processingStringsSemaphore.Release();
                            }
                        }

                        lock (_finishedItems)
                        {
                            haveWritingDuties = false;
                            _someoneWriting = false;
                        }                              
                    }
                }          

                lock(_outstandingThreads)
                {
                    _outstandingThreads.Remove(Thread.CurrentThread);
                    if (0 == _outstandingThreads.Count)
                        _finishedConsumingEvent.Set();
                }
            }

            public IEnumerable<string> Next()
            {
                string s;
                while(null != (s = _reader.ReadLine()))
                    yield return s;
            }
        }

        private static int Main(string[] args)
        {
            ParserResult<CommandLineArguments> o = Parser.Default.ParseArguments<CommandLineArguments>(args);

            return o.MapResult(
                       options =>
            {
                string sidecarJson = File.ReadAllText(options.SideCarFile);
                CLogSidecar textManifest = CLogSidecar.FromJson(sidecarJson, true);
                CLogConfigurationFile config = new CLogConfigurationFile();
                config.TypeEncoders = textManifest.TypeEncoder;

                TextReader file = Console.In;

                if(!string.IsNullOrEmpty(options.BabelTrace))
                {
                    file = new StreamReader(options.BabelTrace);
                }


               // string line;
                LTTNGEventDecoder lttngDecoder = new LTTNGEventDecoder(textManifest);
                int lines = 0;
                StreamWriter outputfile = null;
                if (!String.IsNullOrEmpty(options.OutputFile))
                    outputfile = new StreamWriter(new FileStream(options.OutputFile, FileMode.Create));

                DateTimeOffset startTime = DateTimeOffset.Now;
                ParallelPrinter printer = new ParallelPrinter(x => { return file.ReadLine(); },
                    y => {
                        if (null == outputfile)
                            Console.WriteLine(y);
                        else
                            outputfile.WriteLine(y);
                    },
                    line =>
                    {
                        //action.
                        ++lines;
                        if (0 == lines % 10000)
                        {
                            Console.WriteLine($"Line : {lines}");
                        }
                        Dictionary<string, IClogEventArg> valueBag;
                        CLogDecodedTraceLine bundle = lttngDecoder.DecodedTraceLine(line, out valueBag);
                        string ret = DecodeAndTraceToConsole(outputfile, bundle, line, config, valueBag);
                        return ret;
                    });

               // Console.ReadKey();
                Console.WriteLine($"Decoded {lines} in {DateTimeOffset.Now - startTime}");
                return 0;
            }, err =>
            {
                Console.WriteLine("Bad Args : " + err);
                return -1;
            });
        }

        public interface ICLogEventDecoder
        {
            public string GetValue(string value);
        }

        public class LTTNGEventDecoder
        {
            private readonly CLogSidecar _sidecar;

            public LTTNGEventDecoder(CLogSidecar sidecar)
            {
                _sidecar = sidecar;
            }

            public CLogDecodedTraceLine DecodedTraceLine(string babbleTraceLine, out Dictionary<string, IClogEventArg> args)
            {
                args = SplitBabelTraceLine(babbleTraceLine);

                if(!args.ContainsKey("name") || !args.ContainsKey("event.fields"))
                {
                    Console.WriteLine("TraceHasNoArgs");
                }

                CLogDecodedTraceLine bundle = _sidecar.FindBundle(args["name"].AsString);

                string fields = args["event.fields"].AsString.Substring(1, args["event.fields"].AsString.Length - 2).Trim();

                if(0 == fields.Length)
                {
                    args = new Dictionary<string, IClogEventArg>();
                }
                else
                {
                    args = SplitBabelTraceLine(fields);
                }

                return bundle;
            }
        }

        public class LTTNGClogEvent : IClogEventArg
        {
            public LTTNGClogEvent(string value)
            {
                AsString = value;
            }

            public string AsString
            {
                get;
            }

            public int AsInt32
            {
                get
                {
                    return Convert.ToInt32(AsString);
                }
            }

            public uint AsUInt32
            {
                get
                {
                    return Convert.ToUInt32(AsString);
                }
            }

            public byte[] AsBinary
            {
                get
                {
                    //CLogConsoleTrace.TraceLine(TraceType.Err, "Binary Encoding Not Yet Supported with LTTNG");
                    return new byte[0];
                    //throw new NotImplementedException("Binary Encoding Not Supported");
                }
            }
        }
    }
}
