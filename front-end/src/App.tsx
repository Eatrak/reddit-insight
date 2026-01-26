import { BrowserRouter, Route, Routes } from "react-router-dom";
import Layout from "@/components/layout";
import Dashboard from "@/pages/Dashboard";
import TopicDetail from "@/pages/TopicDetail";

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<Dashboard />} />
          <Route path="/topics/:id" element={<TopicDetail />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

export default App;