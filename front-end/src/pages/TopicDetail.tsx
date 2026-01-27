import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import {
  Bar,
  BarChart,
  Line,
  LineChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { format } from "date-fns";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { api, type Metric, type Topic } from "@/services/api";

export default function TopicDetail() {
  const { id } = useParams<{ id: string }>();
  const [topic, setTopic] = useState<Topic | null>(null);
  const [metrics, setMetrics] = useState<Metric[]>([]);
  const [selectedWindow, setSelectedWindow] = useState<string>("1d");
  const [selectedRange, setSelectedRange] = useState<number>(7);

  useEffect(() => {
    if (id) {
      api.getTopic(id).then(setTopic).catch(console.error);
      api.getTopicReport(id).then((data) => {
        setMetrics(data.metrics);
         // Default to first available window type if 1d not found
         if (data.metrics.length > 0) {
            const has1d = data.metrics.some(m => m.window_type === "1d");
            if (!has1d) {
                setSelectedWindow(data.metrics[0].window_type);
            }
        }
      }).catch(console.error);
    }
  }, [id]);

  if (!topic) {
    return <div className="p-8">Loading topic...</div>;
  }

  // Fixed window types (Resolution)
  const ALL_WINDOW_TYPES = ["1d", "1w", "1m"];
  const availableWindows = new Set(metrics.map(m => m.window_type));

  // Time Range types (Zoom)
  const TIME_RANGES = [
      { label: "7D", value: 7 },
      { label: "30D", value: 30 },
      { label: "90D", value: 90 }
  ];

  // Filter metrics by selected window (Resolution)
  const filteredMetrics = metrics.filter(m => m.window_type === selectedWindow);
  
  // 1. Determine Step Size based on Window
  let stepMs = 24 * 60 * 60 * 1000; // 1d
  if (selectedWindow === "1w") stepMs = 7 * 24 * 60 * 60 * 1000;
  if (selectedWindow === "1m") stepMs = 30 * 24 * 60 * 60 * 1000;

  // Filter and Time Range (Zoom)
  const now = new Date();
  const rangeCutoff = new Date(now.getTime() - (selectedRange * 24 * 60 * 60 * 1000));

  // 2. Generate Complete Timeline (Zero-Filling)
  // We align chart points to the known metrics for accuracy, filling gaps with 0.
  const chartData = [];
  let runner = rangeCutoff.getTime();
  const endTime = now.getTime();

  while (runner <= endTime) {
      // Find a metric that specifically covers this runner time.
      // Metric 'start' and 'end' define the bucket.
      const match = filteredMetrics.find(m => {
          const mStart = new Date(m.start).getTime();
          const mEnd = new Date(m.end).getTime();
          return runner >= mStart && runner < mEnd;
      });

      chartData.push({
          end: runner, 
          mentions: match ? match.mentions : 0,
          engagement: match ? match.engagement : 0,
          velocity: match ? match.velocity : 0,
          acceleration: match ? match.acceleration : 0
      });
      
      runner += stepMs;
  }


  return (
    <div className="flex min-h-screen w-full flex-col bg-muted/40 p-4 md:p-8">
      <div className="mx-auto grid w-full max-w-6xl gap-4">
        <div className="flex items-center justify-between">
            <div>
                <h1 className="text-3xl font-bold tracking-tight">{topic.id}</h1>
                <p className="text-muted-foreground">{topic.description}</p>
            </div>

             {/* Window Selector */}
             <div className="flex items-center gap-2">
                <span className="text-sm text-muted-foreground">Window:</span>
                <div className="flex items-center rounded-lg border bg-card p-1">
                    {ALL_WINDOW_TYPES.map(w => {
                        const isAvailable = availableWindows.has(w);
                        return (
                        <button
                            key={w}
                            disabled={!isAvailable}
                            onClick={() => setSelectedWindow(w)}
                            className={`px-3 py-1 text-sm font-medium rounded-md transition-colors ${
                                selectedWindow === w 
                                ? "bg-primary text-primary-foreground" 
                                : isAvailable 
                                    ? "text-muted-foreground hover:text-foreground"
                                    : "text-muted-foreground/30 cursor-not-allowed"
                            }`}
                        >
                            {w}
                        </button>
                    )})}
                </div>
            </div>

            {/* Range Selector */}
            <div className="flex items-center gap-2 border-l pl-4 ml-2">
                <span className="text-sm text-muted-foreground">Range:</span>
                <div className="flex items-center rounded-lg border bg-card p-1">
                    {TIME_RANGES.map(r => {
                        let isDisabled = false;
                        if (selectedWindow === "1w" && r.value < 14) isDisabled = true; // Need > 1 week
                        if (selectedWindow === "1m" && r.value < 60) isDisabled = true; // Need > 1 month
                        
                        return (
                        <button
                            key={r.label}
                            disabled={isDisabled}
                            onClick={() => setSelectedRange(r.value)}
                            className={`px-3 py-1 text-sm font-medium rounded-md transition-colors ${
                                selectedRange === r.value
                                ? "bg-secondary text-secondary-foreground" 
                                : isDisabled
                                    ? "text-muted-foreground/30 cursor-not-allowed"
                                    : "text-muted-foreground hover:text-foreground"
                            }`}
                        >
                            {r.label}
                        </button>
                    )})}
                </div>
            </div>
        </div>
        
        <div className="flex flex-wrap gap-2">
            {topic.keywords.map(k => (
                <span key={k} className="inline-flex items-center rounded-md bg-blue-50 px-2 py-1 text-xs font-medium text-blue-700 ring-1 ring-inset ring-blue-700/10">
                    {k}
                </span>
            ))}
             {topic.subreddits.map(s => (
                <span key={s} className="inline-flex items-center rounded-md bg-orange-50 px-2 py-1 text-xs font-medium text-orange-700 ring-1 ring-inset ring-orange-700/10">
                    r/{s}
                </span>
            ))}
        </div>

        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Mentions (Sum in View)</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {filteredMetrics.reduce((acc, curr) => acc + curr.mentions, 0)}
              </div>
            </CardContent>
          </Card>
           <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total Engagement</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                 {filteredMetrics.reduce((acc, curr) => acc + curr.engagement, 0)}
              </div>
            </CardContent>
          </Card>
           <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Data Points</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                 {filteredMetrics.length}
              </div>
            </CardContent>
          </Card>
        </div>

        <div className="grid gap-4 md:grid-cols-1">
            <Card className="col-span-1">
              <CardHeader>
                <CardTitle>Mentions Over Time ({selectedWindow})</CardTitle>
              </CardHeader>
              <CardContent className="pl-2">
                 <div className="h-[300px] w-full">
                    <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={chartData}>
                            <XAxis 
                                dataKey="end" 
                                tickFormatter={(val) => format(new Date(val), "MMM d")}
                                stroke="#888888"
                                fontSize={12}
                                tickLine={false}
                                axisLine={false}
                            />
                            <YAxis 
                                stroke="#888888"
                                fontSize={12}
                                tickLine={false}
                                axisLine={false}
                                tickFormatter={(value) => `${value}`}
                            />
                             <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="hsl(var(--border))" />
                            <Tooltip 
                                cursor={{ fill: 'hsl(var(--muted)/0.4)' }}
                                contentStyle={{ backgroundColor: 'hsl(var(--card))', borderColor: 'hsl(var(--border))' }}
                                itemStyle={{ color: 'hsl(var(--foreground))' }}
                                labelFormatter={(label) => format(new Date(label), "PP p")}
                            />
                            <Bar
                                dataKey="mentions" 
                                fill="hsl(var(--primary))" 
                                radius={[4, 4, 0, 0]}
                                activeBar={false}
                            />
                        </BarChart>
                    </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>

             <Card className="col-span-1">
              <CardHeader>
                <CardTitle>Velocity ({selectedWindow})</CardTitle>
              </CardHeader>
               <CardContent className="pl-2">
                 <div className="h-[300px] w-full">
                    <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={chartData}>
                            <XAxis 
                                dataKey="end" 
                                type="number"
                                domain={[rangeCutoff.getTime(), now.getTime()]}
                                tickFormatter={(val) => format(new Date(val), "MMM d")}
                                scale="time"
                                stroke="#888888"
                                fontSize={12}
                                tickLine={false}
                                axisLine={false}
                            />
                            <YAxis 
                                stroke="#888888"
                                fontSize={12}
                                tickLine={false}
                                axisLine={false}
                                tickFormatter={(value) => `${value}`}
                            />
                            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="hsl(var(--border))" />
                            <Tooltip 
                                contentStyle={{ backgroundColor: 'hsl(var(--card))', borderColor: 'hsl(var(--border))' }}
                                itemStyle={{ color: 'hsl(var(--foreground))' }}
                                labelFormatter={(label) => format(new Date(label), "PP p")}
                            />
                             <Line
                                type="linear" 
                                dataKey="velocity" 
                                name="Velocity"
                                stroke="#82ca9d" 
                                strokeWidth={2}
                                dot={{ strokeWidth: 2, r: 4 }}
                                activeDot={{ r: 6 }}
                            />
                        </LineChart>
                    </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>

             <Card className="col-span-1">
              <CardHeader>
                <CardTitle>Acceleration ({selectedWindow})</CardTitle>
              </CardHeader>
               <CardContent className="pl-2">
                 <div className="h-[300px] w-full">
                    <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={chartData}>
                            <XAxis 
                                dataKey="end" 
                                type="number"
                                domain={[rangeCutoff.getTime(), now.getTime()]}
                                tickFormatter={(val) => format(new Date(val), "MMM d")}
                                scale="time"
                                stroke="#888888"
                                fontSize={12}
                                tickLine={false}
                                axisLine={false}
                            />
                            <YAxis 
                                stroke="#888888"
                                fontSize={12}
                                tickLine={false}
                                axisLine={false}
                                tickFormatter={(value) => `${value}`}
                            />
                            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="hsl(var(--border))" />
                            <Tooltip 
                                contentStyle={{ backgroundColor: 'hsl(var(--card))', borderColor: 'hsl(var(--border))' }}
                                itemStyle={{ color: 'hsl(var(--foreground))' }}
                                labelFormatter={(label) => format(new Date(label), "PP p")}
                            />
                             <Line
                                type="linear" 
                                dataKey="acceleration" 
                                name="Acceleration"
                                stroke="#ffc658" 
                                strokeWidth={2}
                                dot={{ strokeWidth: 2, r: 4 }}
                                activeDot={{ r: 6 }}
                            />
                        </LineChart>
                    </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>
        </div>
      </div>
    </div>
  );
}
