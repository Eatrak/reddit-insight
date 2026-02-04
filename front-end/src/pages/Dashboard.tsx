import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { Activity, ArrowRight } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { api, type Topic } from "@/services/api";
import TopicCreator from "@/components/TopicCreator";
import { useInterval } from "@/lib/useInterval";

export default function Dashboard() {
  const [topics, setTopics] = useState<Topic[]>([]);

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    try {
      const topicsData = await api.getTopics();
      setTopics(topicsData);
    } catch (error) {
      console.error("Failed to fetch data:", error);
    }
  };

  const [lastFinishedTopic, setLastFinishedTopic] = useState<string | null>(
    null,
  );
  const [cooldownActive, setCooldownActive] = useState(false);

  useEffect(() => {
    const activeBackfill = topics.find((t) => t.backfill_status === "PENDING");
    if (activeBackfill) {
      setLastFinishedTopic(activeBackfill.id);
    } else if (lastFinishedTopic) {
      // A backfill just finished
      setCooldownActive(true);
      const timer = setTimeout(() => {
        setCooldownActive(false);
        setLastFinishedTopic(null);
      }, 8000); // 8s cooldown
      return () => clearTimeout(timer);
    }
  }, [topics, lastFinishedTopic]);

  // Faster polling (5s) if any topic is backfilling or in cooldown, otherwise 10s
  const isAnyBackfilling = topics.some((t) => t.backfill_status === "PENDING");
  useInterval(fetchData, isAnyBackfilling || cooldownActive ? 5000 : 10000);

  return (
    <div className="flex min-h-screen w-full flex-col bg-muted/40">
      <div className="flex flex-col sm:gap-4 sm:py-4 sm:pl-14">
        <main className="flex-1 items-start gap-4 p-4 sm:px-6 sm:py-0 md:gap-8">
          {/* Topics Management Section */}
          <div className="mx-auto w-full max-w-3xl flex flex-col items-center gap-2">
            <TopicCreator onTopicCreated={fetchData} />

            <Card className="w-full glass mb-2">
              <CardHeader className="flex flex-row items-center">
                <div className="grid gap-2">
                  <CardTitle>Tracked Topics</CardTitle>
                  <CardDescription>
                    Manage the topics you want to monitor on Reddit.
                  </CardDescription>
                </div>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Topic ID</TableHead>
                      <TableHead className="hidden md:table-cell">
                        Status
                      </TableHead>
                      <TableHead className="text-right">Action</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {topics.map((topic) => (
                      <TableRow key={topic.id}>
                        <TableCell>
                          <div className="font-medium">
                            <Link
                              to={`/topics/${topic.id}`}
                              className="hover:underline flex items-center gap-2"
                            >
                              <Activity className="h-4 w-4" />
                              {topic.id}
                            </Link>
                          </div>
                          <div className="hidden text-sm text-muted-foreground md:inline">
                            {topic.description}
                          </div>
                        </TableCell>
                        <TableCell className="hidden md:table-cell">
                          {topic.backfill_status === "PENDING" ? (
                            <span className="inline-flex items-center rounded-md bg-blue-50 px-2 py-1 text-xs font-medium text-blue-700 ring-1 ring-inset ring-blue-700/10">
                              Backfilling{" "}
                              {(topic.backfill_percentage || 0).toFixed(0)}%
                            </span>
                          ) : (
                            <span className="inline-flex items-center rounded-md bg-green-50 px-2 py-1 text-xs font-medium text-green-700 ring-1 ring-inset ring-green-600/20">
                              Active
                            </span>
                          )}
                        </TableCell>
                        <TableCell className="text-right">
                          <Button asChild size="sm" variant="ghost">
                            <Link to={`/topics/${topic.id}`}>
                              Open
                              <ArrowRight className="ml-2 h-4 w-4" />
                            </Link>
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </div>
        </main>
      </div>
    </div>
  );
}
