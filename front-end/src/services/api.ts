import axios from "axios";

const API_BASE_URL = "/api"; // Proxy will handle this

export interface Filter {
  text?: string;
  lang?: string;
  author?: string;
}

export interface Topic {
  id: string;
  description: string;
  keywords: string[];
  subreddits: string[];
  filters: Filter;
  update_frequency_seconds: number;
  is_active: boolean;
  updated_at?: string;
  backfill_status?: string; // IDLE, PENDING, COMPLETED, ERROR
  backfill_percentage?: number;
}

export interface Metric {
  _index: string;
  _id: string;
  _score: number;
  topic_id: string;
  window_type: string;
  start: string;
  end: string;
  mentions: number;
  engagement: number;
  sentiment_positive: number;
  sentiment_negative: number;
  sentiment_neutral: number;
  top_keywords: Array<{ key: string; doc_count: number }>;
  velocity?: number;
  acceleration?: number;
  trend_score?: number;
}

export const api = {
  getTopics: async (): Promise<Topic[]> => {
    const response = await axios.get(`${API_BASE_URL}/topics`);
    return response.data;
  },

  getTopic: async (id: string): Promise<Topic> => {
    const response = await axios.get(`${API_BASE_URL}/topics/${id}`);
    return response.data;
  },

  createTopic: async (
    topic: Partial<Topic>,
  ): Promise<{ message: string; id: string }> => {
    const response = await axios.post(`${API_BASE_URL}/topics`, topic);
    return response.data;
  },

  getTopicReport: async (
    id: string,
  ): Promise<{ topic_id: string; metrics: Metric[] }> => {
    const response = await axios.get(`${API_BASE_URL}/topics/${id}/report`);
    return response.data;
  },

  generateConfig: async (description: string): Promise<Partial<Topic>> => {
    const response = await axios.post(`${API_BASE_URL}/generate-config`, {
      description,
    });
    return response.data;
  },

  generateRandomPrompt: async (): Promise<{ prompt: string }> => {
    const response = await axios.post(`${API_BASE_URL}/generate-random-prompt`);
    return response.data;
  },

  getSubreddits: async (): Promise<{ subreddits: string[] }> => {
    const response = await axios.get(`${API_BASE_URL}/subreddits`);
    return response.data;
  },

  triggerBackfill: async (id: string): Promise<void> => {
    await axios.post(`${API_BASE_URL}/topics/${id}/backfill`);
  },
};
