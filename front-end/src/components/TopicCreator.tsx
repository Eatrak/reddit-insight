import { useEffect, useState } from "react";
import { Check, Loader2, Search, Sparkles, Wand2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { api } from "@/services/api";
import { TopicRuleBuilder } from "./TopicRuleBuilder";

interface TopicCreatorProps {
  onTopicCreated: () => void;
}

export default function TopicCreator({ onTopicCreated }: TopicCreatorProps) {
  const [isReviewOpen, setIsReviewOpen] = useState(false);
  const [isSubredditPickerOpen, setIsSubredditPickerOpen] = useState(false);
  const [description, setDescription] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [availableSubreddits, setAvailableSubreddits] = useState<string[]>([]);
  const [subredditSearch, setSubredditSearch] = useState("");

  useEffect(() => {
    const fetchSubreddits = async () => {
      try {
        const { subreddits } = await api.getSubreddits();
        setAvailableSubreddits(subreddits);
      } catch (error) {
        console.error("Failed to fetch subreddits:", error);
      }
    };
    fetchSubreddits();
  }, []);

  // Config state
  const [config, setConfig] = useState({
    id: "",
    keywords: [] as string[][], // Converted to CNF: [[A, B], [C]] -> (A or B) AND C.
    subreddits: "",
    description: "",
  });

  const handleRandomPrompt = async () => {
    setIsLoading(true);
    try {
      const result = await api.generateRandomPrompt();
      const cleanPrompt = result.prompt
        .replace(/^Prompt:\s*/i, "")
        .replace(/reddit\.com\/r\//gi, "r/")
        .replace(/reddit\.com/gi, "")
        .replace(/https?:\/\//gi, "");
      setDescription(cleanPrompt.trim());
    } catch (error) {
      console.error("Failed to generate prompt:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleGenerate = async () => {
    if (!description.trim()) return;

    setIsLoading(true);
    try {
      const result = await api.generateConfig(description);

      // Parse keywords flexibly to support potential legacy/simple formats
      let rawKeywords = result.keywords || [];
      let parsedGroups: string[][] = [];

      if (Array.isArray(rawKeywords)) {
        if (rawKeywords.length > 0 && Array.isArray(rawKeywords[0])) {
          // Already CNF: [["a", "b"], ["c"]]
          parsedGroups = rawKeywords as string[][];
        } else {
          // Flat list: ["a", "b"] -> Treat as ONE group (OR) -> [["a", "b"]]
          // OR Treat as Separate groups (AND)?
          // Logic: If flat list, usually means synonymous terms. So -> [["a", "b"]]
          const flat = rawKeywords as string[];
          if (flat.length > 0) parsedGroups = [flat];
        }
      }

      setConfig({
        id: result.id || "",
        keywords: parsedGroups,
        subreddits: Array.isArray(result.subreddits)
          ? result.subreddits.join(", ")
          : result.subreddits || "",
        description: result.description || description,
      });
      setIsReviewOpen(true);
    } catch (error) {
      console.error("Generation failed:", error);
      alert("Failed to generate configuration. Please try again.");
    } finally {
      setIsLoading(false);
    }
  };

  const handleSave = async () => {
    try {
      // Filter out empty groups/terms
      const cleanedKeywords = config.keywords
        .map((g) => g.filter((t) => t.trim() !== ""))
        .filter((g) => g.length > 0);

      await api.createTopic({
        id: config.id,
        description: config.description,
        keywords: cleanedKeywords, // Send as native JSON array
        subreddits: config.subreddits.split(",").map((s) => s.trim()),
        update_frequency_seconds: 60,
        is_active: true, // Default to active
      });

      // Reset and notify parent
      setIsReviewOpen(false);
      setDescription("");
      setConfig({ id: "", keywords: [], subreddits: "", description: "" });
      onTopicCreated();
    } catch (error) {
      console.error("Failed to save topic:", error);
      alert("Failed to save topic. ID might already exist.");
    }
  };

  return (
    <div className="flex flex-col items-center justify-center space-y-8 py-6 w-full">
      <div className="text-center space-y-2">
        <h1 className="text-3xl font-bold tracking-tight">Create New Topic</h1>
        <p className="text-muted-foreground text-lg">
          Describe what you want to track, and AI will configure it for you.
        </p>
      </div>

      <Card className="w-full shadow-2xl border-0 glass">
        <CardContent className="pt-6 space-y-4">
          <Textarea
            placeholder="e.g. I want to monitor discussions about the release of GTA VI and leaks..."
            className="min-h-[150px] text-lg resize-none p-4"
            value={description}
            onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) =>
              setDescription(e.target.value)
            }
          />

          <div className="flex items-center justify-between">
            <Button
              size="sm"
              onClick={handleRandomPrompt}
              className="gap-2 btn-glow border-0 text-white font-medium hover:opacity-90"
            >
              <Sparkles className="h-4 w-4" />
              Surprise Me
            </Button>

            <Button
              size="lg"
              onClick={handleGenerate}
              disabled={isLoading || !description.trim()}
              className="gap-2"
            >
              {isLoading ? (
                <>
                  <Loader2 className="h-5 w-5 animate-spin" />
                  Thinking...
                </>
              ) : (
                <>
                  <Wand2 className="h-5 w-5" />
                  Generate Config
                </>
              )}
            </Button>
          </div>
        </CardContent>
      </Card>

      <Dialog open={isReviewOpen} onOpenChange={setIsReviewOpen}>
        <DialogContent className="sm:max-w-[600px] max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Review Configuration</DialogTitle>
            <DialogDescription>
              Review the generated settings before saving.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-6 py-4">
            <div className="grid gap-2">
              <Label htmlFor="id">Topic ID</Label>
              <Input
                id="id"
                value={config.id}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  setConfig({ ...config, id: e.target.value })
                }
              />
              <p className="text-xs text-muted-foreground">
                Unique identifier (kebab-case).
              </p>
            </div>

            <div className="grid gap-2">
              <Label htmlFor="desc">Description</Label>
              <Input
                id="desc"
                value={config.description}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                  setConfig({ ...config, description: e.target.value })
                }
              />
            </div>

            <div className="grid gap-2">
              <TopicRuleBuilder
                groups={config.keywords}
                onChange={(newGroups) =>
                  setConfig({ ...config, keywords: newGroups })
                }
              />
            </div>

            <div className="grid gap-2">
              <Label>Subreddits</Label>
              <div className="flex flex-wrap gap-2 p-3 border rounded-md bg-muted/20 min-h-[40px]">
                {config.subreddits
                  .split(",")
                  .map((s) => s.trim())
                  .filter((s) => s !== "")
                  .map((sub) => (
                    <div
                      key={typeof sub === "string" ? sub : JSON.stringify(sub)}
                      className="bg-primary/10 text-primary px-2 py-1 rounded text-sm flex items-center gap-1"
                    >
                      {typeof sub === "string"
                        ? sub
                        : (sub as any).name || JSON.stringify(sub)}
                      <button
                        onClick={() => {
                          const subs = config.subreddits
                            .split(",")
                            .map((s) => s.trim())
                            .filter((s) => s !== sub);
                          setConfig({ ...config, subreddits: subs.join(", ") });
                        }}
                        className="hover:text-destructive"
                      >
                        ×
                      </button>
                    </div>
                  ))}
                <Button
                  variant="outline"
                  size="sm"
                  className="h-7 text-xs"
                  onClick={() => setIsSubredditPickerOpen(true)}
                >
                  {config.subreddits ? "+ Add/Change" : "Select Subreddits"}
                </Button>
              </div>
              <p className="text-xs text-muted-foreground">
                Select relevant subreddits from the allowed list.
              </p>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setIsReviewOpen(false)}>
              Cancel
            </Button>
            <Button onClick={handleSave}>Create Topic</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <Dialog
        open={isSubredditPickerOpen}
        onOpenChange={setIsSubredditPickerOpen}
      >
        <DialogContent className="sm:max-w-[400px]">
          <DialogHeader>
            <DialogTitle>Select Subreddits</DialogTitle>
            <DialogDescription className="flex justify-between items-center">
              <span>Choose subreddits from the allowed list.</span>
              <span
                className={`text-xs font-semibold px-2 py-0.5 rounded ${
                  config.subreddits.split(",").filter((s) => s.trim()).length >=
                  30
                    ? "bg-destructive/10 text-destructive"
                    : "bg-muted text-muted-foreground"
                }`}
              >
                Selected:{" "}
                {config.subreddits.split(",").filter((s) => s.trim()).length}/30
              </span>
            </DialogDescription>
          </DialogHeader>
          <div className="relative mb-4">
            <Search className="absolute left-3 top-3 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search or filter subreddits..."
              className="pl-9"
              value={subredditSearch}
              onChange={(e) => setSubredditSearch(e.target.value)}
            />
          </div>
          <div className="max-h-[400px] overflow-y-auto space-y-1 p-1">
            {availableSubreddits.length === 0 && (
              <p className="text-center text-muted-foreground py-4">
                Loading subreddits...
              </p>
            )}
            {availableSubreddits
              .filter((s) =>
                s.toLowerCase().includes(subredditSearch.toLowerCase()),
              )
              .map((sub) => {
                const isSelected = config.subreddits
                  .split(",")
                  .map((s) => s.trim())
                  .includes(sub);
                return (
                  <label
                    key={sub}
                    className={`flex items-center justify-between px-3 py-2 rounded-md cursor-pointer transition-colors hover:bg-muted ${isSelected ? "bg-primary/10 text-primary font-medium" : ""}`}
                  >
                    <span>{sub}</span>
                    <input
                      type="checkbox"
                      className="hidden"
                      checked={isSelected}
                      onChange={() => {
                        let subs = config.subreddits
                          .split(",")
                          .map((s) => s.trim())
                          .filter((s) => s !== "");

                        if (isSelected) {
                          subs = subs.filter((s) => s !== sub);
                        } else {
                          if (subs.length >= 30) {
                            alert(
                              "You can select a maximum of 30 subreddits per topic.",
                            );
                            return;
                          }
                          subs = [...subs, sub];
                        }
                        setConfig({ ...config, subreddits: subs.join(", ") });
                      }}
                    />
                    {isSelected && <Check className="h-4 w-4" />}
                  </label>
                );
              })}
          </div>
          <DialogFooter>
            <Button
              className="w-full"
              onClick={() => setIsSubredditPickerOpen(false)}
            >
              Done
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
