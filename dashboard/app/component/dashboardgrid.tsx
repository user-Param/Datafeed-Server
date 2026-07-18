"use client";

import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import GridLayout, { Layout, verticalCompactor, useContainerWidth } from 'react-grid-layout';
import 'react-grid-layout/css/styles.css';
import 'react-resizable/css/styles.css';

interface DashboardGridProps {
  children: React.ReactNode;
  onLayoutChange?: (layout: Layout) => void;
  cols?: number;
  rowHeight?: number;
  containerPadding?: [number, number];
}

const DashboardGrid: React.FC<DashboardGridProps> = ({
  children,
  onLayoutChange,
  cols = 12,
  rowHeight = 60,
  containerPadding = [0, 0],
}) => {
  const [layout, setLayout] = useState<Layout>([]);
  const isLayoutLoaded = useRef(false);
  const { width, containerRef } = useContainerWidth();

  const childrenArray = useMemo(() => React.Children.toArray(children), [children]);

  // Load saved layout or generate default (only once)
  useEffect(() => {
    if (isLayoutLoaded.current) return;

    const saved = localStorage.getItem('dashboard-layout');
    if (saved) {
      try {
        const parsed = JSON.parse(saved);
        if (Array.isArray(parsed) && parsed.length > 0) {
          console.log('[DashboardGrid] Loaded saved layout:', parsed);
          setLayout(parsed);
          isLayoutLoaded.current = true;
          return;
        }
      } catch (err) {
        console.warn('[DashboardGrid] Failed to parse saved layout:', err);
      }
    }

    // No saved layout → generate default
    console.log('[DashboardGrid] Generating default layout');
    const defaultLayout = childrenArray.map((child, i) => {
      const element = child as React.ReactElement<{ id?: string }>;
      const id = element?.props?.id || `card-${i}`;
      return {
        i: id,
        x: (i * 4) % cols,
        y: Math.floor(i / (cols / 4)) * 2,
        w: 3,
        h: 3,
        minW: 3,
        minH: 3,
      };
    });
    setLayout(defaultLayout);
    isLayoutLoaded.current = true;
  }, [childrenArray, cols]);

  // Update internal state when grid changes (but DO NOT save here)
  const handleLayoutChange = useCallback((newLayout: Layout) => {
    setLayout(newLayout);
    if (onLayoutChange) onLayoutChange(newLayout);
  }, [onLayoutChange]);

  // Save to localStorage ONLY when user drags or resizes
  const handleSaveLayout = useCallback((newLayout: Layout) => {
    console.log('[DashboardGrid] Saving layout to localStorage:', newLayout);
    localStorage.setItem('dashboard-layout', JSON.stringify(newLayout));
    if (onLayoutChange) onLayoutChange(newLayout);
  }, [onLayoutChange]);

  return (
    <div ref={containerRef} className="w-full">
      <GridLayout
        className="layout"
        layout={layout}
        onLayoutChange={handleLayoutChange}
        onDragStop={handleSaveLayout}      // ✅ Save after drag
        onResizeStop={handleSaveLayout}    // ✅ Save after resize
        width={width}
        cols={cols}
        rowHeight={rowHeight}
        containerPadding={containerPadding}
        draggableHandle=".drag-handle"
        isDraggable={true}
        isResizable={true}
        compactor={verticalCompactor}
        autoSize={true}
        // Disable auto-compaction to avoid unwanted changes on load
        compactType={null}
      >
        {childrenArray.map((child, idx) => {
          if (!React.isValidElement(child)) return null;
          const element = child as React.ReactElement<{ id?: string }>;
          const id = element?.props?.id || `card-${idx}`;
          return (
            <div key={id} data-grid={{ i: id }}>
              {child}
            </div>
          );
        })}
      </GridLayout>
    </div>
  );
};

export default DashboardGrid;