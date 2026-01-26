import { useState } from "react";
import { Loader2, Sparkles, Wand2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { api } from "@/services/api";

interface TopicCreatorProps {
  onTopicCreated: () => void;
}

export default function TopicCreator({ onTopicCreated }: TopicCreatorProps) {
  const [step, setStep] = useState<"prompt" | "review">("prompt");
  const [description, setDescription] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  
  // Config state
  const [config, setConfig] = useState({
    id: "",
    keywords: "",
    subreddits: "",
    description: "",
  });

  const handleRandomPrompt = async () => {
    setIsLoading(true);
    try {
      const result = await api.generateRandomPrompt();
      setDescription(result.prompt);
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
      setConfig({
        id: result.id || "",
        keywords: Array.isArray(result.keywords) ? result.keywords.join(", ") : (result.keywords || ""),
        subreddits: Array.isArray(result.subreddits) ? result.subreddits.join(", ") : (result.subreddits || ""),
        description: result.description || description,
      });
      setStep("review");
    } catch (error) {
      console.error("Generation failed:", error);
      alert("Failed to generate configuration. Please try again.");
    } finally {
      setIsLoading(false);
    }
  };

  const handleSave = async () => {
    try {
      await api.createTopic({
        id: config.id,
        description: config.description,
        keywords: config.keywords.split(",").map(k => k.trim()),
        subreddits: config.subreddits.split(",").map(s => s.trim()),
        update_frequency_seconds: 60,
        is_active: true, // Default to active
      });
      
      // Reset and notify parent
      setStep("prompt");
      setDescription("");
      setConfig({ id: "", keywords: "", subreddits: "", description: "" });
      onTopicCreated();
      
    } catch (error) {
      console.error("Failed to save topic:", error);
      alert("Failed to save topic. ID might already exist.");
    }
  };

  if (step === "prompt") {
    return (
      <div className="flex flex-col items-center justify-center space-y-8 py-6 w-full">
        <div className="text-center space-y-2">
          <h1 className="text-3xl font-bold tracking-tight">Create New Topic</h1>
          <p className="text-muted-foreground text-lg">
            Describe what you want to track, and AI will configure it for you.
          </p>
        </div>

        <Card className="w-full shadow-lg border-primary/20">
          <CardContent className="pt-6 space-y-4">
            <Textarea
              placeholder="e.g. I want to monitor discussions about the release of GTA VI and leaks..."
              className="min-h-[150px] text-lg resize-none p-4"
              value={description}
              onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => setDescription(e.target.value)}
            />
            
            <div className="flex items-center justify-between">
              <Button variant="outline" size="sm" onClick={handleRandomPrompt} className="gap-2 text-muted-foreground">
                <Sparkles className="h-4 w-4" />
                Surprise Me
              </Button>
              
              <Button size="lg" onClick={handleGenerate} disabled={isLoading || !description.trim()} className="gap-2">
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
      </div>
    );
  }

  return (
    <Card className="xl:col-span-2 animate-in fade-in slide-in-from-bottom-4 duration-500">
      <CardHeader>
        <CardTitle>Review Configuration</CardTitle>
        <CardDescription>
          Review the generated settings before saving.
        </CardDescription>
      </CardHeader>
      <CardContent className="grid gap-6">
        <div className="grid gap-2">
          <Label htmlFor="id">Topic ID</Label>
          <Input 
            id="id" 
            value={config.id} 
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setConfig({...config, id: e.target.value})}
          />
          <p className="text-xs text-muted-foreground">Unique identifier (kebab-case).</p>
        </div>
        
        <div className="grid gap-2">
          <Label htmlFor="desc">Description</Label>
          <Input 
            id="desc" 
            value={config.description} 
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setConfig({...config, description: e.target.value})}
          />
        </div>

        <div className="grid gap-2">
          <Label htmlFor="keywords">Keywords</Label>
          <Textarea 
            id="keywords" 
            value={config.keywords} 
            onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => setConfig({...config, keywords: e.target.value})}
          />
          <p className="text-xs text-muted-foreground">Comma-separated list of terms to track.</p>
        </div>

        <div className="grid gap-2">
          <Label htmlFor="subreddits">Subreddits</Label>
          <Input 
            id="subreddits" 
            value={config.subreddits} 
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setConfig({...config, subreddits: e.target.value})}
          />
          <p className="text-xs text-muted-foreground">Comma-separated list of subreddits.</p>
        </div>
      </CardContent>
      <div className="p-6 pt-0 flex justify-end gap-2">
          <Button variant="ghost" onClick={() => setStep("prompt")}>Edit Prompt</Button>
          <Button onClick={handleSave}>Create Topic</Button>
      </div>
    </Card>
  );
}
