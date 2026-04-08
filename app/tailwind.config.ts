import type { Config } from "tailwindcss";

export default {
  content: ["./client/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        rag: {
          red: "#DC2626",
          amber: "#F59E0B",
          green: "#16A34A",
        },
        water: {
          50: "#EFF6FF",
          100: "#DBEAFE",
          200: "#BFDBFE",
          600: "#2563EB",
          700: "#1D4ED8",
          800: "#1E3A5F",
          900: "#0F172A",
        },
      },
    },
  },
  plugins: [],
} satisfies Config;
